use crate::array::{LamellarRead, LamellarWrite};
use crate::lamellae::{AllocationType, Lamellae};
use crate::lamellar_team::LamellarTeamRemotePtr;
use crate::memregion::*;
use crate::IdError;
use crate::LamellarTeamRT;
use crate::LAMELLAES;
// use crate::active_messaging::AmDist;

use core::marker::PhantomData;
use parking_lot::Mutex;
use serde::ser::Serialize;
use std::collections::HashMap;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
// use tracing::*;
// use serde::ser::{Serialize, Serializer, SerializeStruct};

lazy_static! {
    pub(crate) static ref ONE_SIDED_MEM_REGIONS: Mutex<HashMap<(usize, usize), Arc<MemRegionHandleInner>>> =
        Mutex::new(HashMap::new());
}

static ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(serde::Serialize, serde::Deserialize)]
struct NetMemRegionHandle {
    mr_addr: usize,
    mr_size: usize,
    mr_pe: usize,
    team: LamellarTeamRemotePtr,
    my_id: (usize, usize),
    parent_id: (usize, usize),
}

impl From<NetMemRegionHandle> for Arc<MemRegionHandleInner> {
    fn from(net_handle: NetMemRegionHandle) -> Self {
        // let net_handle: NetMemRegionHandle = Deserialize::deserialize(remote).expect("error deserializing, expected NetMemRegionHandle");
        // println!("received handle: pid {:?} gpid{:?}",net_handle.my_id,net_handle.parent_id);
        let grand_parent_id = net_handle.parent_id;
        let parent_id = net_handle.my_id;
        let lamellae = if let Some(lamellae) = LAMELLAES.read().get(&net_handle.team.backend) {
            lamellae.clone()
        } else {
            panic!(
                "unexepected lamellae backend {:?}",
                &net_handle.team.backend
            );
        };
        let mut mrh_map = ONE_SIDED_MEM_REGIONS.lock();
        // for elem in mrh_map.iter(){
        //     println!("elem: {:?}",elem);
        // }
        let mrh = match mrh_map.get(&parent_id) {
            Some(mrh) => mrh.clone(),
            None => {
                let local_mem_region_addr = lamellae.local_addr(parent_id.1, net_handle.mr_addr); //the address is with respect to the PE that sent the memregion handle
                let mem_region = MemoryRegion::from_remote_addr(
                    local_mem_region_addr,
                    net_handle.mr_pe,
                    net_handle.mr_size,
                    lamellae.clone(),
                )
                .unwrap();
                let team: Pin<Arc<LamellarTeamRT>> = net_handle.team.into();
                let mrh = Arc::new(MemRegionHandleInner {
                    mr: mem_region,
                    team: team.clone(),
                    local_ref: AtomicUsize::new(0),
                    remote_sent: AtomicUsize::new(0),
                    remote_recv: AtomicUsize::new(0),
                    my_id: (
                        ID_COUNTER.fetch_add(1, Ordering::Relaxed),
                        team.team_pe.expect("pe not part of team"),
                    ),
                    parent_id: parent_id,
                    grand_parent_id: grand_parent_id,
                    local_dropped: AtomicBool::new(false),
                });
                mrh_map.insert(parent_id, mrh.clone());
                // println!("inserting onesided mem region {:?} {:?} 0x{:x} {:?}",parent_id,net_handle.mr_pe,net_handle.mr_addr,mrh);
                mrh
            }
        };
        mrh.local_ref.fetch_add(1, Ordering::SeqCst);
        // println!("recived mrh: {:?}",mrh);
        mrh
    }
}

impl From<Arc<MemRegionHandleInner>> for NetMemRegionHandle {
    fn from(mem_reg: Arc<MemRegionHandleInner>) -> Self {
        // println!("creating net handle {:?}",mem_reg);
        NetMemRegionHandle {
            mr_addr: mem_reg.mr.addr,
            mr_size: mem_reg.mr.size,
            mr_pe: mem_reg.mr.pe,
            team: mem_reg.team.clone().into(),
            my_id: mem_reg.my_id,
            parent_id: mem_reg.parent_id,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MemRegionHandleInner {
    mr: MemoryRegion<u8>,
    team: Pin<Arc<LamellarTeamRT>>,
    local_ref: AtomicUsize,
    remote_sent: AtomicUsize,
    remote_recv: AtomicUsize,
    my_id: (usize, usize),           //id,pe
    parent_id: (usize, usize),       //id, parent pe
    grand_parent_id: (usize, usize), //id, grand parent pe
    local_dropped: AtomicBool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct MemRegionHandle {
    #[serde(with = "memregion_handle_serde")]
    inner: Arc<MemRegionHandleInner>,
}

pub(crate) mod memregion_handle_serde {
    use serde::Serialize;
    use std::sync::Arc;

    pub(crate) fn serialize<S>(
        inner: &Arc<super::MemRegionHandleInner>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let nethandle = super::NetMemRegionHandle::from(inner.clone());
        // println!("nethandle {:?} {:?}",crate::serialized_size(&nethandle,false),crate::serialized_size(&nethandle,true));
        nethandle.serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Arc<super::MemRegionHandleInner>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // println!("in deserialize memregion_handle_serde");
        let net_handle: super::NetMemRegionHandle = serde::Deserialize::deserialize(deserializer)?;
        Ok(net_handle.into())
    }
}

impl crate::active_messaging::DarcSerde for MemRegionHandle {
    fn ser(&self, num_pes: usize) {
        // match cur_pe {
        //     Ok(cur_pe) => {
        self.inner.remote_sent.fetch_add(num_pes, Ordering::SeqCst);
        //     }
        //     Err(err) => {
        //         panic!("can only access MemRegionHandles within team members ({:?})", err);
        //     }
        // }
    }
    fn des(&self, _cur_pe: Result<usize, IdError>) {
        // match cur_pe {
        //     Ok(cur_pe) => {
        self.inner.remote_recv.fetch_add(1, Ordering::SeqCst);
        //     }
        //     Err(err) => {
        //         panic!("can only access MemRegionHandles within team members ({:?})", err);
        //     }
        // }
        // println!("deserailized mrh: {:?}",self.inner);
    }
}

impl Clone for MemRegionHandle {
    fn clone(&self) -> Self {
        self.inner.local_ref.fetch_add(1, Ordering::SeqCst);
        MemRegionHandle {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for MemRegionHandle {
    fn drop(&mut self) {
        //this means all local instances of this handle have been dropped

        let mut mrh_map = ONE_SIDED_MEM_REGIONS.lock();
        let cnt = self.inner.local_ref.fetch_sub(1, Ordering::SeqCst);
        // println!("mem region dropping {:?}",self.inner);
        if cnt == 1
            && self
                .inner
                .local_dropped
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            //last local reference (for the first time)
            // println!("last local ref {:?}", self.inner);
            if self.inner.remote_sent.load(Ordering::SeqCst) == 0 {
                mrh_map.remove(&self.inner.parent_id);
                // println!("removed {:?}",self.inner);
                if self.inner.my_id != self.inner.parent_id {
                    let cnt = self.inner.remote_recv.swap(0, Ordering::SeqCst);
                    if cnt > 0 {
                        let temp = MemRegionFinishedAm {
                            cnt: cnt,
                            parent_id: self.inner.grand_parent_id,
                        };
                        // println!("sending finished am {:?} pe: {:?}",temp, self.inner.parent_id.1);
                        self.inner.team.exec_am_pe(self.inner.parent_id.1, temp);
                    }
                }
            } else {
                //need to wait for references I sent to return
                self.inner.team.exec_am_local(MemRegionDropWaitAm {
                    inner: self.inner.clone(),
                });
            }
        }
    }
}

#[lamellar_impl::AmDataRT(Debug)]
struct MemRegionFinishedAm {
    cnt: usize,
    parent_id: (usize, usize),
}

#[lamellar_impl::rt_am]
impl LamellarAM for MemRegionFinishedAm {
    async fn exec(self) {
        // println!("in finished am {:?}",self);
        let mrh_map = ONE_SIDED_MEM_REGIONS.lock();
        let _mrh = match mrh_map.get(&self.parent_id) {
            Some(mrh) => {
                mrh.remote_sent.fetch_sub(self.cnt, Ordering::SeqCst);
                // println!("in finished am {:?} mrh {:?}",self,mrh);
            }
            None => println!(
                "in finished am this should only be possible on the original pe? {:?} ",
                self
            ), //or we are on the original node?
        };
        // println!("leaving finished am");
    }
}

#[lamellar_impl::AmLocalDataRT(Debug)]
struct MemRegionDropWaitAm {
    inner: Arc<MemRegionHandleInner>,
}

#[lamellar_impl::rt_am_local]
impl LamellarAM for MemRegionDropWaitAm {
    async fn exec(self) {
        // println!("in drop wait {:?}", self.inner);
        loop {
            while self.inner.remote_sent.load(Ordering::SeqCst) != 0
                || self.inner.local_ref.load(Ordering::SeqCst) != 0
            {
                async_std::task::yield_now().await;
            }
            {
                //drop the mrh_map lock before awaiting
                let mut mrh_map = ONE_SIDED_MEM_REGIONS.lock();
                //check counts again because requests could have come in by the time we can lock the map
                if self.inner.remote_sent.load(Ordering::SeqCst) == 0
                    && self.inner.local_ref.load(Ordering::SeqCst) == 0
                {
                    mrh_map.remove(&self.inner.parent_id);
                    // println!("waited removed {:?}",self.inner);
                    if self.inner.my_id != self.inner.parent_id {
                        let cnt = self.inner.remote_recv.swap(0, Ordering::SeqCst);
                        if cnt > 0 {
                            let temp = MemRegionFinishedAm {
                                cnt: cnt,
                                parent_id: self.inner.grand_parent_id,
                            };
                            // println!("waited sending finished am {:?} pe: {:?}",temp, self.inner.parent_id.1);
                            self.inner.team.exec_am_pe(self.inner.parent_id.1, temp);
                        }
                    }
                    break;
                }
            }
            async_std::task::yield_now().await;
        }
        // println!("leaving drop wait {:?}", self.inner);
    }
}

/// A OneSided Memory Region is a [RemoteMemoryRegion] that has only been allocated on a single PE.
///
/// The memory region provides RDMA access to any PE which has a handle to the region.
///
/// OneSidedMemoryRegions implement distributed reference counting, so their handles can be sent along in active messages
/// to other Remote PE's, and it is gauranteed that the memory region will remain valid as long as a single reference
/// exists on any PE anywhere in the distributed system (even if the original allocating PE drops all local references to the memory region)
///
/// OneSidedMemoryRegions are constructed using either the LamellarWorld instance or a LamellarTeam instance.
///
/// Memory Regions are low-level unsafe abstraction not really intended for use in higher-level applications
///
/// # Warning
/// Unless you are very confident in low level distributed memory access or you explicitly need a one sided memory region it is highly recommended you utilize the
/// [LamellarArray][crate::array::LamellarArray]  interface to construct and interact with distributed memory.
///
/// # Examples
///
///
///```
/// use lamellar::{OneSidedMemoryRegion, RemoteMemoryRegion};
///
/// let world_mem_region: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(1000);
/// let team_mem_region: OneSidedMemoryRegion<usize> = some_team.alloc_one_sided_mem_region(1000);
/// ```
#[lamellar_impl::AmDataRT(Clone)]
pub struct OneSidedMemoryRegion<T: Dist> {
    mr: MemRegionHandle,
    pe: usize, // the original pe
    sub_region_offset: usize,
    sub_region_size: usize,
    phantom: PhantomData<T>,
}

impl<T: Dist> OneSidedMemoryRegion<T> {
    // pub(crate) fn new(
    //     size: usize,
    //     team: &std::pin::Pin<Arc<LamellarTeamRT>>,
    //     lamellae: Arc<Lamellae>,
    // ) -> OneSidedMemoryRegion<T> {
    //     OneSidedMemoryRegion::try_new(size, team, lamellae).expect("out of memory")
    // }
    pub(crate) fn try_new(
        size: usize,
        team: &std::pin::Pin<Arc<LamellarTeamRT>>,
        lamellae: Arc<Lamellae>,
    ) -> Result<OneSidedMemoryRegion<T>, anyhow::Error> {
        let mr = MemoryRegion::try_new(
            size * std::mem::size_of::<T>(),
            lamellae,
            AllocationType::Local,
        )?;
        let pe = mr.pe;

        let id = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mrh = MemRegionHandle {
            inner: Arc::new(MemRegionHandleInner {
                mr: mr,
                team: team.clone(),
                local_ref: AtomicUsize::new(1),
                remote_sent: AtomicUsize::new(0),
                remote_recv: AtomicUsize::new(0),
                my_id: (id, pe),
                parent_id: (id, pe),
                grand_parent_id: (id, pe),
                local_dropped: AtomicBool::new(false),
            }),
        };

        // println!("new local memory region {:?} ", mrh);

        ONE_SIDED_MEM_REGIONS
            .lock()
            .insert(mrh.inner.my_id, mrh.inner.clone());
        Ok(OneSidedMemoryRegion {
            mr: mrh,
            pe: pe,
            sub_region_offset: 0,
            sub_region_size: size,
            phantom: PhantomData,
        })
    }
    /// The length (in number of elements of `T`) of the local segment of the memory region (i.e. not the global length of the memory region)  
    ///
    /// # Example
    ///```
    /// use lamellar::OneSidedMemoryRegion;
    ///
    /// let mem_region: OneSidedMemoryRegion<usize> = world.alloc_one_sided_mem_region(1000);
    /// assert_eq!(mem_region.len(),1000)
    pub fn len(&self) -> usize {
        RegisteredMemoryRegion::<T>::len(self)
    }

    /// "Puts" (copies) data from a local memory location into a remote memory location on the specified PE
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - A LamellarMemoryRegion (either shared or onesided) that has data local to this PE,
    /// the data buffer may not be safe to upon return from this call, currently the user is responsible for completion detection,
    /// or you may use the similar iput call (with a potential performance penalty);
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    ///
    /// # Panics
    /// Panics if "data" does not have any local data on this PE
    /// Panics if index is out of bounds
    /// Panics if PE is out of bounds
    pub unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        MemoryRegionRDMA::<T>::put(self, self.pe, index, data);
    }

    /// Blocking "Puts" (copies) data from a local memory location into a remote memory location on the specified PE.
    ///
    /// This function blocks until the data in the data buffer has been transfered out of this PE, this does not imply that it has arrived at the remote destination though
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - A LamellarMemoryRegion (either shared or onesided) that has data local to this PE,
    /// the data buffer is free to be reused upon return of this function.
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    ///
    /// # Panics
    /// Panics if "data" does not have any local data on this PE
    /// Panics if index is out of bounds
    /// Panics if PE is out of bounds
    pub unsafe fn blocking_put<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        MemoryRegionRDMA::<T>::blocking_put(self, self.pe, index, data);
    }

    /// "Puts" (copies) data from a local memory location into a remote memory location on all PEs containing the memory region
    ///
    /// This is similar to broad cast
    ///
    /// # Arguments
    ///
    /// * `index` - offset into the remote memory window
    /// * `data` - A LamellarMemoryRegion (either shared or onesided) that has data local to this PE,
    /// the data buffer may not be safe to upon return from this call, currently the user is responsible for completion detection,
    /// or you may use the similar iput call (with a potential performance penalty);
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied the data buffer
    ///
    /// # Panics
    /// Panics if "data" does not have any local data on this PE
    /// Panics if index is out of bounds
    pub unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        MemoryRegionRDMA::<T>::put_all(self, index, data);
    }

    /// "Gets" (copies) data from remote memory location on the specified PE into the provided data buffer.
    /// After calling this function, the data may or may not have actually arrived into the data buffer.
    /// The user is responsible for transmission termination detection
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - A LamellarMemoryRegion (either shared or onesided) that has data local to this PE
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    /// Additionally, when this call returns the underlying fabric provider may or may not have already copied data into the data buffer.
    ///
    /// # Panics
    /// Panics if "data" does not have any local data on this PE
    /// Panics if index is out of bounds
    /// Panics if PE is out of bounds
    pub unsafe fn get_unchecked<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        MemoryRegionRDMA::<T>::get_unchecked(self, self.pe, index, data);
    }

    /// Blocking "Gets" (copies) data from remote memory location on the specified PE into the provided data buffer.
    /// After calling this function, the data is guaranteed to be placed in the data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - A LamellarMemoryRegion (either shared or onesided) that has data local to this PE
    ///
    /// # Safety
    /// This call is always unsafe as mutual exclusitivity is not enforced, i.e. many other reader/writers can exist simultaneously.
    ///
    /// # Panics
    /// Panics if "data" does not have any local data on this PE
    /// Panics if index is out of bounds
    /// Panics if PE is out of bounds
    pub unsafe fn blocking_get<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        MemoryRegionRDMA::<T>::blocking_get(self, self.pe, index, data);
    }

    /// Return a slice of the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    pub unsafe fn as_slice(&self) -> MemResult<&[T]> {
        RegisteredMemoryRegion::<T>::as_slice(self)
    }

    /// Return a reference to the local (to the calling PE) element located by the provided index
    ///
    /// Returns an error if the index is out of bounds or the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    pub unsafe fn at(&self, index: usize) -> MemResult<&T> {
        RegisteredMemoryRegion::<T>::at(self, index)
    }

    /// Return a mutable slice of the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist other mutable references elsewhere in the distributed system.
    pub unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        RegisteredMemoryRegion::<T>::as_mut_slice(self)
    }

    /// Return a ptr to the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    pub unsafe fn as_ptr(&self) -> MemResult<*const T> {
        RegisteredMemoryRegion::<T>::as_ptr(self)
    }

    /// Return a mutable ptr to the local (to the calling PE) data of the memory region
    ///
    /// Returns an error if the PE does not contain any local data associated with this memory region
    ///
    /// # Safety
    /// this call is always unsafe as there is no gaurantee that there do not exist mutable references elsewhere in the distributed system.
    pub unsafe fn as_mut_ptr(&self) -> MemResult<*mut T> {
        RegisteredMemoryRegion::<T>::as_mut_ptr(self)
    }

    /// Create a sub region of this OneSidedMemoryRegion using the provided range
    ///
    /// # Panics
    /// panics if the end range is larger than the length of the memory region
    pub fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> OneSidedMemoryRegion<T> {
        let start = match range.start_bound() {
            //inclusive
            Bound::Included(idx) => *idx,
            Bound::Excluded(idx) => *idx + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            //exclusive
            Bound::Included(idx) => *idx + 1,
            Bound::Excluded(idx) => *idx,
            Bound::Unbounded => self.sub_region_size,
        };
        if end > self.sub_region_size {
            panic!(
                "subregion range ({:?}-{:?}) exceeds size of memregion {:?}",
                start, end, self.sub_region_size
            );
        }

        // println!("local subregion: {:?} {:?} {:?}",start,end,(end-start));
        OneSidedMemoryRegion {
            mr: self.mr.clone(),
            pe: self.pe,
            sub_region_offset: self.sub_region_offset + start,
            sub_region_size: (end - start),
            phantom: PhantomData,
        }
    }

    /// Convert from a memregion of type `T` into one of type `B`
    /// # Saftey
    /// this is highly unsafe, DO NOT USE THIS.
    /// Because Dist has Copy as a trait bound, the runtime uses this to convert back and forth between u8 and T for use in some of the automatically generated Active Messages
    #[doc(hidden)]
    pub unsafe fn to_base<B: Dist>(self) -> OneSidedMemoryRegion<B> {
        let u8_offset = self.sub_region_offset * std::mem::size_of::<T>();
        let u8_size = self.sub_region_size * std::mem::size_of::<T>();
        OneSidedMemoryRegion {
            mr: self.mr.clone(),
            pe: self.pe,
            sub_region_offset: u8_offset / std::mem::size_of::<B>(),
            sub_region_size: u8_size / std::mem::size_of::<B>(),
            phantom: PhantomData,
        }
    }

    /// An iterator to data local to this PE
    ///
    /// # Panics
    /// Panics if the calling PE does not contain any local data
    pub unsafe fn iter(&self) -> std::slice::Iter<'_, T> {
        self.as_slice().unwrap().iter()
    }

    /// Checks for if the calling PE contains any local data
    ///
    /// Returns true if the PE does contain data, false otherwise
    pub fn data_local(&self) -> bool {
        if self.pe == self.mr.inner.my_id.1 {
            if let Ok(_addr) = self.mr.inner.mr.addr() {
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}

impl<T: Dist + serde::Serialize> OneSidedMemoryRegion<T> {
    pub(crate) fn serialize_local_data<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        unsafe { self.as_slice().unwrap().serialize(s) }
    }
}

impl<T: Dist> RegisteredMemoryRegion<T> for OneSidedMemoryRegion<T> {
    fn len(&self) -> usize {
        self.sub_region_size
    }
    fn addr(&self) -> MemResult<usize> {
        if self.pe == self.mr.inner.my_id.1 {
            if let Ok(addr) = self.mr.inner.mr.addr() {
                Ok(addr + self.sub_region_offset * std::mem::size_of::<T>())
            } else {
                Err(MemNotLocalError {})
            }
        } else {
            Err(MemNotLocalError {})
        }
    }
    unsafe fn at(&self, index: usize) -> MemResult<&T> {
        self.mr.inner.mr.casted_at::<T>(index)
    }

    unsafe fn as_slice(&self) -> MemResult<&[T]> {
        if self.pe == self.mr.inner.my_id.1 {
            if let Ok(slice) = self.mr.inner.mr.as_casted_slice::<T>() {
                Ok(&slice[self.sub_region_offset..(self.sub_region_offset + self.sub_region_size)])
            } else {
                Err(MemNotLocalError {})
            }
        } else {
            Err(MemNotLocalError {})
        }
    }
    unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        // println!("pe {:?} mr_pe {:?}",self.pe , self.mr.inner.my_id.1);
        if self.pe == self.mr.inner.my_id.1 {
            if let Ok(slice) = self.mr.inner.mr.as_casted_mut_slice::<T>() {
                Ok(&mut slice
                    [self.sub_region_offset..(self.sub_region_offset + self.sub_region_size)])
            } else {
                Err(MemNotLocalError {})
            }
        } else {
            Err(MemNotLocalError {})
        }
    }
    unsafe fn as_ptr(&self) -> MemResult<*const T> {
        if self.pe == self.mr.inner.my_id.1 {
            if let Ok(addr) = self.addr() {
                Ok(addr as *const T)
            } else {
                Err(MemNotLocalError {})
            }
        } else {
            Err(MemNotLocalError {})
        }
    }
    unsafe fn as_mut_ptr(&self) -> MemResult<*mut T> {
        if self.pe == self.mr.inner.my_id.1 {
            if let Ok(addr) = self.addr() {
                Ok(addr as *mut T)
            } else {
                Err(MemNotLocalError {})
            }
        } else {
            Err(MemNotLocalError {})
        }
    }
}

impl<T: Dist> MemRegionId for OneSidedMemoryRegion<T> {
    fn id(&self) -> usize {
        self.mr.inner.mr.id()
    }
}

impl<T: Dist> SubRegion<T> for OneSidedMemoryRegion<T> {
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarMemoryRegion<T> {
        self.sub_region(range).into()
    }
}

impl<T: Dist> AsBase for OneSidedMemoryRegion<T> {
    unsafe fn to_base<B: Dist>(self) -> LamellarMemoryRegion<B> {
        self.to_base::<B>().into()
    }
}

impl<T: Dist> MemoryRegionRDMA<T> for OneSidedMemoryRegion<T> {
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U) {
        if self.pe == pe {
            self.mr
                .inner
                .mr
                .put(pe, self.sub_region_offset + index, data);
        } else {
            panic!(
                "trying to put to PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
    unsafe fn blocking_put<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        if self.pe == pe {
            self.mr
                .inner
                .mr
                .blocking_put(pe, self.sub_region_offset + index, data);
        // self.mr.iput(pe, index, data);
        } else {
            panic!(
                "trying to put to PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U) {
        self.mr
            .inner
            .mr
            .put_all(self.sub_region_offset + index, data);
    }
    unsafe fn get_unchecked<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        if self.pe == pe {
            self.mr
                .inner
                .mr
                .get_unchecked(pe, self.sub_region_offset + index, data);
        } else {
            panic!(
                "trying to get from PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
    unsafe fn blocking_get<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        if self.pe == pe {
            self.mr
                .inner
                .mr
                .blocking_get(pe, self.sub_region_offset + index, data);
        } else {
            panic!(
                "trying to get from PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
}

impl<T: Dist> RTMemoryRegionRDMA<T> for OneSidedMemoryRegion<T> {
    unsafe fn put_slice(&self, pe: usize, index: usize, data: &[T]) {
        if self.pe == pe {
            self.mr
                .inner
                .mr
                .put_slice(pe, self.sub_region_offset + index, data)
        } else {
            panic!(
                "trying to put to PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
    unsafe fn blocking_get_slice(&self, pe: usize, index: usize, data: &mut [T]) {
        if self.pe == pe {
            self.mr
                .inner
                .mr
                .blocking_get_slice(pe, self.sub_region_offset + index, data)
        } else {
            panic!(
                "trying to put to PE {:?} which does not contain data (pe with data =  {:?})",
                pe, self.pe
            );
            // Err(MemNotLocalError {})
        }
    }
}

impl<T: Dist> std::fmt::Debug for OneSidedMemoryRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{:?}] local mem region:  {:?} ",
            self.pe, self.mr.inner.mr,
        )
    }
}

impl<T: Dist> LamellarWrite for OneSidedMemoryRegion<T> {}
impl<T: Dist> LamellarWrite for &OneSidedMemoryRegion<T> {}
impl<T: Dist> LamellarRead for OneSidedMemoryRegion<T> {}

impl<T: Dist> From<&OneSidedMemoryRegion<T>> for LamellarMemoryRegion<T> {
    fn from(smr: &OneSidedMemoryRegion<T>) -> Self {
        LamellarMemoryRegion::Local(smr.clone())
    }
}

impl<T: Dist> From<&OneSidedMemoryRegion<T>> for LamellarArrayInput<T> {
    fn from(smr: &OneSidedMemoryRegion<T>) -> Self {
        LamellarArrayInput::LocalMemRegion(smr.clone())
    }
}

impl<T: Dist> MyFrom<&OneSidedMemoryRegion<T>> for LamellarArrayInput<T> {
    fn my_from(smr: &OneSidedMemoryRegion<T>, _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarArrayInput::LocalMemRegion(smr.clone())
    }
}

impl<T: Dist> MyFrom<OneSidedMemoryRegion<T>> for LamellarArrayInput<T> {
    fn my_from(smr: OneSidedMemoryRegion<T>, _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarArrayInput::LocalMemRegion(smr)
    }
}

// pub(crate) struct OneSidedMemoryRegionIter<'a,T: Dist>{
//     inner:
// }
