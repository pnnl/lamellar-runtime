use crate::array::{LamellarArrayInput, MyFrom};
use crate::lamellae::{AllocationType, Backend, Lamellae, LamellaeComm, LamellaeRDMA};
use crate::lamellar_team::LamellarTeam;
// use crate::lamellar_array::{LamellarLocalArray};
use core::marker::PhantomData;
#[cfg(feature = "enable-prof")]
use lamellar_prof::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use std::ops::{Bound, RangeBounds};

pub(crate) mod shared;
use shared::SharedMemoryRegion;

pub(crate) mod local;
use local::LocalMemoryRegion;

use enum_dispatch::enum_dispatch;

#[derive(Debug, Clone)]
pub struct MemNotLocalError;

pub type MemResult<T> = Result<T, MemNotLocalError>;

impl std::fmt::Display for MemNotLocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "mem region not local",)
    }
}

impl std::error::Error for MemNotLocalError {}
pub trait Dist: //serde::ser::Serialize
//+ serde::de::DeserializeOwned
std::clone::Clone
+ Send
+ Sync {}

impl<T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync> Dist
    for T
{
}

#[enum_dispatch(RegisteredMemoryRegion<T>, MemRegionId, AsBase, SubRegion<T>, MemoryRegionRDMA<T>, RTMemoryRegionRDMA<T>)]
// #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[derive( Clone, Debug)]
pub enum LamellarMemoryRegion<T: Dist + 'static> {
    Shared(SharedMemoryRegion<T>),
    Local(LocalMemoryRegion<T>),
}

impl<T: Dist + 'static> From<&LamellarMemoryRegion<T>> for LamellarArrayInput<T> {
    fn from(mr: &LamellarMemoryRegion<T>) -> Self {
        LamellarArrayInput::LamellarMemRegion(mr.clone())
    }
}

impl<T: Dist + 'static> MyFrom<&LamellarMemoryRegion<T>> for LamellarArrayInput<T> {
    fn my_from(mr: &LamellarMemoryRegion<T>, _team: &Arc<LamellarTeam>) -> Self {
        LamellarArrayInput::LamellarMemRegion(mr.clone())
    }
}

#[enum_dispatch]
pub trait RegisteredMemoryRegion<T: Dist + 'static> {
    fn len(&self) -> usize;
    fn addr(&self) -> MemResult<usize>;
    fn as_slice(&self) -> MemResult<&[T]>;
    unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]>;
    fn as_ptr(&self) -> MemResult<*const T>;
    fn as_mut_ptr(&self) -> MemResult<*mut T>;
}

#[enum_dispatch]
pub(crate) trait MemRegionId {
    fn id(&self) -> usize;
}

// we seperate SubRegion and AsBase out as their own traits
// because we want MemRegion to impl RegisteredMemoryRegion (so that it can be used in Shared + Local)
// but MemRegion should not return LamellarMemoryRegions directly (as both SubRegion and AsBase require)
// we will implement seperate functions for MemoryRegion itself.
#[enum_dispatch]
pub trait SubRegion<T: Dist + 'static> {
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarMemoryRegion<T>;
}

#[enum_dispatch]
pub(crate) trait AsBase {
    unsafe fn as_base<B: Dist + 'static>(self) -> LamellarMemoryRegion<B>;
}

#[enum_dispatch]
pub trait MemoryRegionRDMA<T: Dist + 'static> {
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U);
    fn iput<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U);
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U);
    unsafe fn get<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U);
}

#[enum_dispatch]
pub(crate) trait RTMemoryRegionRDMA<T: Dist + 'static> {
    unsafe fn put_slice(&self, pe: usize, index: usize, data: &[T]);
}

//#[prof]
impl<T: Dist + 'static> Hash for LamellarMemoryRegion<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

//#[prof]
impl<T: Dist + 'static> PartialEq for LamellarMemoryRegion<T> {
    fn eq(&self, other: &LamellarMemoryRegion<T>) -> bool {
        self.id() == other.id()
    }
}

//#[prof]
impl<T: Dist + 'static> Eq for LamellarMemoryRegion<T> {}

// this is not intended to be accessed directly by a user
// it will be wrapped in either a shared region or local region
// in shared regions its wrapped in a darc which allows us to send
// to different nodes, in local we dont currently support serialization
// #[derive(serde::Serialize, serde::Deserialize)]
// #[serde(into = "__NetworkMemoryRegion<T>", from = "__NetworkMemoryRegion<T>")]
pub(crate) struct MemoryRegion<T: Dist + 'static> {
    addr: usize,
    pe: usize,
    size: usize,
    backend: Backend,
    rdma: Arc<dyn LamellaeRDMA>,
    local: bool,
    phantom: PhantomData<T>,
}

impl<T: Dist + 'static> MemoryRegion<T> {
    pub(crate) fn new(
        size: usize, //number of elements of type T
        lamellae: Arc<Lamellae>,
        alloc: AllocationType,
    ) -> MemoryRegion<T> {
        let cnt = Arc::new(AtomicUsize::new(1));
        ACTIVE.insert(lamellae.backend(), lamellae.clone(), cnt.clone());
        // let rdma = lamellae.clone();
        // println!("creating new lamellar memory region {:?}",size * std::mem::size_of::<T>());
        let mut local = false;
        let addr = if size > 0 {
            if let AllocationType::Local = alloc {
                local = true;
                lamellae.rt_alloc(size * std::mem::size_of::<T>()).unwrap() + lamellae.base_addr()
            } else {
                lamellae
                    .alloc(size * std::mem::size_of::<T>(), alloc)
                    .unwrap() //did we call team barrer before this?
            }
        } else {
            0
        };
        let temp = MemoryRegion {
            addr: addr,
            pe: lamellae.my_pe(),
            size: size,
            backend: lamellae.backend(),
            rdma: lamellae,
            local: local,
            phantom: PhantomData,
        };
        // println!(
        //     "new memregion {:x} {:x}",
        //     temp.addr,
        //     size * std::mem::size_of::<T>()
        // );
        temp
    }

    //remove... subregions of a raw memory region is not possible, need to use shared or local wrappers
    pub(crate) fn sub_region<R: RangeBounds<usize>>(&self, range: R) -> MemoryRegion<T> {
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
            Bound::Unbounded => self.size,
        };
        // self.cnt.fetch_add(1, Ordering::SeqCst);
        // println!("subregion {:?} {:?} {:?} {:x?} {:x?}",start,end,end-start,self.addr+start,self.addr+(start+(end-start)-1)*std::mem::size_of::<T>());
        MemoryRegion {
            addr: self.addr + (start * std::mem::size_of::<T>()),
            pe: self.pe,
            size: (end - start),
            backend: self.backend,
            rdma: self.rdma.clone(),
            local: self.local,
            phantom: self.phantom,
        }
    }

    pub(crate) unsafe fn as_base<B: Dist + 'static>(self) -> MemoryRegion<B> {
        //this is allowed as we consume the old object..
        let num_bytes = self.size * std::mem::size_of::<T>();
        assert_eq!(
            num_bytes % std::mem::size_of::<B>(),
            0,
            "Error converting memregion to new base, does not align"
        );
        MemoryRegion {
            addr: self.addr, //TODO: out of memory...
            pe: self.pe,
            size: num_bytes / std::mem::size_of::<B>(),
            backend: self.backend,
            rdma: self.rdma.clone(),
            local: self.local,
            phantom: PhantomData,
        }
    }
    // }

    //#[prof]
    // impl<T: Dist + 'static> MemoryRegionRDMA<T> for MemoryRegion<T> {
    /// copy data from local memory location into a remote memory location
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    /// the data buffer may not be safe to upon return from this call, currently the user is responsible for completion detection,
    /// or you may use the similar iput call (with a potential performance penalty);
    pub(crate) unsafe fn put<R: Dist + 'static, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        //todo make return a result?
        let data = data.into();
        if index + data.len() <= self.size {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_ptr() {
                let bytes = std::slice::from_raw_parts(ptr as *const u8, num_bytes);
                self.rdma
                    .put(pe, bytes, self.addr + index * std::mem::size_of::<T>())
            } else {
                panic!("ERROR: put data src is not local");
            }
        } else {
            println!("{:?} {:?} {:?}", self.size, index, data.len());
            panic!("index out of bounds");
        }
    }

    /// copy data from local memory location into a remote memory localtion
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    /// the data buffer is free to be reused upon return of this function.
    pub(crate) fn iput<R: Dist + 'static, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        //todo make return a result?
        let data = data.into();
        if index + data.len() <= self.size {
            let num_bytes = data.len() * std::mem::size_of::<T>();
            if let Ok(ptr) = data.as_ptr() {
                let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, num_bytes) };
                self.rdma
                    .iput(pe, bytes, self.addr + index * std::mem::size_of::<T>())
            } else {
                panic!("ERROR: put data src is not local");
            }
        } else {
            println!("{:?} {:?} {:?}", self.size, index, data.len());
            panic!("index out of bounds");
        }
    }

    pub(crate) unsafe fn put_all<R: Dist + 'static, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        index: usize,
        data: U,
    ) {
        let data = data.into();
        if index + data.len() <= self.size {
            let num_bytes = data.len() * std::mem::size_of::<T>();
            if let Ok(ptr) = data.as_ptr() {
                let bytes = std::slice::from_raw_parts(ptr as *const u8, num_bytes);
                self.rdma
                    .put_all(bytes, self.addr + index * std::mem::size_of::<T>());
            } else {
                panic!("ERROR: put data src is not local");
            }
        } else {
            panic!("index out of bounds");
        }
    }

    //TODO: once we have a reliable asynchronos get wait mechanism, we return a request handle,
    //data probably needs to be referenced count or lifespan controlled so we know it exists when the get trys to complete
    //in the handle drop method we will wait until the request completes before dropping...  ensuring the data has a place to go
    /// copy data from remote memory location into provided data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of destination buffer to store result of the get
    pub(crate) unsafe fn get<R: Dist + 'static, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        let data = data.into();
        if index + data.len() <= self.size {
            let num_bytes = data.len() * std::mem::size_of::<T>();
            if let Ok(ptr) = data.as_mut_ptr() {
                let bytes = std::slice::from_raw_parts_mut(ptr as *mut u8, num_bytes);
                self.rdma
                    .get(pe, self.addr + index * std::mem::size_of::<T>(), bytes);
            //(remote pe, src, dst)
            // println!("getting {:?} {:?} [{:?}] {:?} {:?} {:?}",pe,self.addr + index * std::mem::size_of::<T>(),index,data.addr(),data.len(),num_bytes);
            } else {
                panic!("ERROR: get data dst is not local");
            }
        } else {
            println!("{:?} {:?} {:?}", self.size, index, data.len(),);
            panic!("index out of bounds");
        }
    }
    // }

    // impl<T: Dist + 'static> RTMemoryRegionRDMA<T> for MemoryRegion<T> {
    pub(crate) unsafe fn put_slice<R: Dist + 'static>(&self, pe: usize, index: usize, data: &[R]) {
        //todo make return a result?
        if index + data.len() <= self.size {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            let bytes = std::slice::from_raw_parts(data.as_ptr() as *const u8, num_bytes);
            // println!(
            //     "mem region len: {:?} index: {:?} data len{:?} num_bytes {:?}  from {:?} to {:x} ({:x} [{:?}])",
            //     self.size,
            //     index,
            //     data.len(),
            //     num_bytes,
            //     data.as_ptr(),
            //     self.addr,
            //     self.addr + index * std::mem::size_of::<T>(),
            //     pe,
            // );
            self.rdma
                .put(pe, bytes, self.addr + index * std::mem::size_of::<T>())
        } else {
            println!(
                "mem region len: {:?} index: {:?} data len{:?}",
                self.size,
                index,
                data.len()
            );
            panic!("index out of bounds");
        }
    }
    // }

    // impl<T: Dist + 'static> RegisteredMemoryRegion<T> for MemoryRegion<T> {
    pub(crate) fn len(&self) -> usize {
        self.size
    }
    pub(crate) fn addr(&self) -> MemResult<usize> {
        Ok(self.addr)
    }
    pub(crate) fn as_slice(&self) -> MemResult<&[T]> {
        if self.addr != 0 {
            Ok(unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) })
        } else {
            Ok(&[])
        }
    }
    pub(crate) fn as_casted_slice<R: Dist + 'static>(&self) -> MemResult<&[R]> {
        if self.addr != 0 {
            let num_bytes = self.size * std::mem::size_of::<T>();
            assert_eq!(
                num_bytes % std::mem::size_of::<R>(),
                0,
                "Error converting memregion to new base, does not align"
            );
            Ok(unsafe {
                std::slice::from_raw_parts(
                    self.addr as *const R,
                    num_bytes / std::mem::size_of::<R>(),
                )
            })
        } else {
            Ok(&[])
        }
    }
    pub(crate) unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        if self.addr != 0 {
            Ok(std::slice::from_raw_parts_mut(
                self.addr as *mut T,
                self.size,
            ))
        } else {
            Ok(&mut [])
        }
    }
    pub(crate) unsafe fn as_casted_mut_slice<R: Dist + 'static>(&self) -> MemResult<&mut [R]> {
        if self.addr != 0 {
            let num_bytes = self.size * std::mem::size_of::<T>();
            assert_eq!(
                num_bytes % std::mem::size_of::<R>(),
                0,
                "Error converting memregion to new base, does not align"
            );
            Ok(std::slice::from_raw_parts_mut(
                self.addr as *mut R,
                num_bytes / std::mem::size_of::<R>(),
            ))
        } else {
            Ok(&mut [])
        }
    }
    pub(crate) fn as_ptr(&self) -> MemResult<*const T> {
        Ok(self.addr as *const T)
    }
    pub(crate) fn as_casted_ptr<R: Dist + 'static>(&self) -> MemResult<*const R> {
        Ok(self.addr as *const R)
    }
    pub(crate) fn as_mut_ptr(&self) -> MemResult<*mut T> {
        Ok(self.addr as *mut T)
    }
    pub(crate) fn as_casted_mut_ptr<R: Dist + 'static>(&self) -> MemResult<*mut R> {
        Ok(self.addr as *mut R)
    }
}

impl<T: Dist + 'static> MemRegionId for MemoryRegion<T> {
    fn id(&self) -> usize {
        self.addr //probably should be key
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy)]
pub struct __NetworkMemoryRegion<T: Dist + 'static> {
    // orig_addr: usize,
    addr: usize,
    pe: usize,
    size: usize,
    backend: Backend,
    local: bool,
    phantom: PhantomData<T>,
}

lazy_static! {
    static ref ACTIVE: CountedHashMap = CountedHashMap::new();
}

struct CountedHashMap {
    lock: RwLock<CountedHashMapInner>,
}
unsafe impl Send for CountedHashMap {}

struct CountedHashMapInner {
    cnts: HashMap<Backend, usize>,
    lamellaes: HashMap<Backend, (Arc<Lamellae>, Arc<AtomicUsize>)>,
}

//#[prof]
impl CountedHashMap {
    pub fn new() -> CountedHashMap {
        CountedHashMap {
            lock: RwLock::new(CountedHashMapInner {
                cnts: HashMap::new(),
                lamellaes: HashMap::new(),
            }),
        }
    }
    #[allow(dead_code)]
    pub fn print(&self) {
        for (k, v) in self.lock.read().lamellaes.iter() {
            println!(
                "backend: {:?} {:?} {:?}",
                k,
                Arc::strong_count(&v.0),
                v.1.load(Ordering::Relaxed)
            );
        }
    }

    pub fn insert(&self, backend: Backend, lamellae: Arc<Lamellae>, cnt: Arc<AtomicUsize>) {
        let mut map = self.lock.write();
        let mut insert = false;
        *map.cnts.entry(backend).or_insert_with(|| {
            insert = true;
            0
        }) += 1;
        if insert {
            map.lamellaes.insert(backend, (lamellae, cnt));
        }
    }

    pub fn remove(&self, backend: Backend) {
        let mut map = self.lock.write();
        if let Some(cnt) = map.cnts.get_mut(&backend) {
            *cnt -= 1;
            if *cnt == 0 {
                map.lamellaes.remove(&backend);
                map.cnts.remove(&backend);
            }
        } else {
            panic!("trying to remove key that does not exist");
        }
    }

    pub fn get(&self, backend: Backend) -> (Arc<Lamellae>, Arc<AtomicUsize>) {
        let map = self.lock.read();
        map.lamellaes.get(&backend).expect("invalid key").clone()
    }
}

pub trait RemoteMemoryRegion {
    /// allocate a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    fn alloc_shared_mem_region<T: Dist + std::marker::Sized + 'static>(
        &self,
        size: usize,
    ) -> SharedMemoryRegion<T>;

    /// allocate a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    fn alloc_local_mem_region<T: Dist + std::marker::Sized + 'static>(
        &self,
        size: usize,
    ) -> LocalMemoryRegion<T>;

    /// release a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `region` - the region to free
    ///
    fn free_shared_memory_region<T: Dist + 'static>(&self, region: SharedMemoryRegion<T>);

    /// release a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `region` - the region to free
    ///
    fn free_local_memory_region<T: Dist + 'static>(&self, region: LocalMemoryRegion<T>);
}

//#[prof]
// impl<T: Dist + 'static> Clone for MemoryRegion<T> {
//     fn clone(&self) -> Self {
//         let mr = MemoryRegion {
//             addr: self.addr,
//             pe: self.pe,
//             size: self.size,
//             backend: self.backend,
//             rdma: self.rdma.clone(),
//             local: self.local,
//             phantom: self.phantom,
//         };
//         // println!("clone: {:?}",lmr);
//         // println!("nlmr: addr: {:?} size: {:?} backend {:?}, lmr: addr: {:?} size: {:?} backend {:?}",reg.addr,reg.size,reg.backend,lmr.addr,lmr.size,lmr.backend);
//         mr
//     }
// }

// impl<T: Dist + 'static> Drop for MemoryRegion<T> {
//     fn drop(&mut self) {
//         // println!("trying to dropping mem region {:?}",self);
//         if self.addr != 0 {
//             if self.local {
//                 self.rdma.rt_free(self.addr - self.rdma.base_addr()); // - self.rdma.base_addr());
//             } else {
//                 self.rdma.free(self.addr);
//             }
//         }
//         //println!("dropping mem region {:?}",self);
//     }
// }

impl<T: Dist + 'static> From<MemoryRegion<T>> for __NetworkMemoryRegion<T> {
    fn from(reg: MemoryRegion<T>) -> Self {
        let nmr = __NetworkMemoryRegion {
            // orig_addr: reg.orig_addr,
            addr: reg.addr, //- ACTIVE.get(reg.backend).0.get_rdma().local_addr(reg.key), //.base_addr(), //remove if we are able to use two separate memory regions
            pe: reg.pe,
            size: reg.size,
            backend: reg.backend,
            local: reg.local,
            phantom: reg.phantom,
        };
        // println!("lmr: addr: {:x} pe: {:?} size: {:?} backend {:?}, nlmr: addr: {:x} pe: {:?} size: {:?} backend {:?}",reg.addr,reg.pe,reg.size,reg.backend,nlmr.addr,nlmr.pe,nlmr.size,nlmr.backend);
        nmr
    }
}

//#[prof]
impl<T: Dist + 'static> From<__NetworkMemoryRegion<T>> for MemoryRegion<T> {
    fn from(reg: __NetworkMemoryRegion<T>) -> Self {
        let temp = ACTIVE.get(reg.backend);
        temp.1.fetch_add(1, Ordering::SeqCst);
        let rdma = temp.0;
        let addr = if reg.addr != 0 {
            rdma.local_addr(reg.pe, reg.addr)
        } else {
            0
        };
        let mr = MemoryRegion {
            // orig_addr: rdma.local_addr(reg.pe, reg.orig_addr),
            addr: addr,
            pe: rdma.my_pe(),
            size: reg.size,
            backend: reg.backend,
            rdma: rdma,
            local: reg.local,
            phantom: reg.phantom,
        };
        // println!("nlmr: addr {:x} pe: {:?} size: {:?} backend {:?}, lmr: addr: {:x} pe: {:?} size: {:?} backend {:?} cnt {:?}",reg.addr,reg.pe,reg.size,reg.backend,lmr.addr,lmr.pe,lmr.size,lmr.backend, lmr.cnt.load(Ordering::SeqCst));
        mr
    }
}

// #[prof]
impl<T: Dist + 'static> std::fmt::Debug for MemoryRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
        // write!(f, "{:?}", slice)
        write!(
            f,
            "addr {:#x} size {:?} backend {:?}", // cnt: {:?}",
            self.addr,
            self.size,
            self.backend,
            // self.cnt.load(Ordering::SeqCst)
        )
    }
}
