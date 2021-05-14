use crate::lamellae::{AllocationType, Backend, Lamellae, LamellaeComm, LamellaeRDMA};
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

#[derive(Debug, Clone)]
pub struct MemNotLocalError;

type MemResult<T> = Result<T, MemNotLocalError>;

impl std::fmt::Display for MemNotLocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "mem region not local",)
    }
}

impl std::error::Error for MemNotLocalError {}

pub trait RegisteredMemoryRegion {
    type Output: std::clone::Clone + Send + Sync + 'static;
    fn len(&self) -> usize;
    fn addr(&self) -> MemResult<usize>;
    fn as_slice(&self) -> MemResult<&[Self::Output]>;
    unsafe fn as_mut_slice(&self) -> MemResult<&mut [Self::Output]>;
    fn as_ptr(&self) -> MemResult<*const Self::Output>;
    fn as_mut_ptr(&self) -> MemResult<*mut Self::Output>;
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self;
}

impl<T: std::clone::Clone + Send + Sync + 'static> RegisteredMemoryRegion
    for LamellarMemoryRegion<T>
{
    type Output = T;
    fn len(&self) -> usize {
        self.size
    }
    fn addr(&self) -> MemResult<usize> {
        Ok(self.addr)
    }
    fn as_slice(&self) -> MemResult<&[T]> {
        Ok(unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) })
    }
    unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        Ok(std::slice::from_raw_parts_mut(
            self.addr as *mut T,
            self.size,
        ))
    }
    fn as_ptr(&self) -> MemResult<*const T> {
        Ok(self.addr as *const T)
    }
    fn as_mut_ptr(&self) -> MemResult<*mut T> {
        Ok(self.addr as *mut T)
    }
    fn sub_region<R: RangeBounds<usize>>(&self, range: R) -> LamellarMemoryRegion<T> {
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
        self.cnt.fetch_add(1, Ordering::SeqCst);
        // println!("subregion {:?} {:?} {:?} {:x?} {:x?}",start,end,end-start,self.addr+start,self.addr+(start+(end-start)-1)*std::mem::size_of::<T>());
        LamellarMemoryRegion {
            addr: self.addr + (start * std::mem::size_of::<T>()),
            pe: self.pe,
            size: (end - start),
            backend: self.backend,
            rdma: self.rdma.clone(),
            cnt: self.cnt.clone(),
            local: self.local,
            phantom: self.phantom,
        }
    }
}

lazy_static! {
    static ref ACTIVE: CountedHashMap = CountedHashMap::new();
}

struct CountedHashMap {
    lock: RwLock<CountedHashMapInner>,
}
// unsafe impl Send for CountedHashMap {}

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
    pub fn print(&self){
        for (k,v) in self.lock.read().lamellaes.iter(){
            println!("backend: {:?} {:?} {:?}",k,Arc::strong_count(&v.0),v.1.load(Ordering::Relaxed));
        }
    }

    pub fn insert(
        &self,
        backend: Backend,
        lamellae: Arc<Lamellae>,
        cnt: Arc<AtomicUsize>,
    ) {
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
    fn alloc_shared_mem_region<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + std::marker::Sized
            + 'static,
    >(
        &self,
        size: usize,
    ) -> LamellarMemoryRegion<T>;

    /// allocate a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    fn alloc_local_mem_region<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + std::marker::Sized
            + 'static,
    >(
        &self,
        size: usize,
    ) -> LamellarLocalMemoryRegion<T>;

    /// release a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `region` - the region to free
    ///
    fn free_shared_memory_region<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + 'static,
    >(
        &self,
        region: LamellarMemoryRegion<T>,
    );

    /// release a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `region` - the region to free
    ///
    fn free_local_memory_region<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + 'static,
    >(
        &self,
        region: LamellarLocalMemoryRegion<T>,
    );
}

pub trait MemoryRegion: std::fmt::Debug {
    fn id(&self) -> usize;
}

//#[prof]
impl Hash for Box<dyn MemoryRegion> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

//#[prof]
impl PartialEq for Box<dyn MemoryRegion> {
    fn eq(&self, other: &Box<dyn MemoryRegion>) -> bool {
        self.id() == other.id()
    }
}

//#[prof]
impl Eq for Box<dyn MemoryRegion> {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy)]
pub struct __NetworkLamellarMemoryRegion<T: std::clone::Clone + Send + Sync + 'static> {
    // orig_addr: usize,
    addr: usize,
    pe: usize,
    size: usize,
    backend: Backend,
    local: bool,
    phantom: PhantomData<T>,
}

//#[prof]
impl<T: std::clone::Clone + Send + Sync + 'static> From<LamellarMemoryRegion<T>>
    for __NetworkLamellarMemoryRegion<T>
{
    fn from(reg: LamellarMemoryRegion<T>) -> Self {
        let nlmr = __NetworkLamellarMemoryRegion {
            // orig_addr: reg.orig_addr,
            addr: reg.addr, //- ACTIVE.get(reg.backend).0.get_rdma().local_addr(reg.key), //.base_addr(), //remove if we are able to use two separate memory regions
            pe: reg.pe,
            size: reg.size,
            backend: reg.backend,
            local: reg.local,
            phantom: reg.phantom,
        };
        // println!("lmr: addr: {:x} pe: {:?} size: {:?} backend {:?}, nlmr: addr: {:x} pe: {:?} size: {:?} backend {:?}",reg.addr,reg.pe,reg.size,reg.backend,nlmr.addr,nlmr.pe,nlmr.size,nlmr.backend);
        nlmr
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(
    into = "__NetworkLamellarMemoryRegion<T>",
    from = "__NetworkLamellarMemoryRegion<T>"
)]
pub struct LamellarMemoryRegion<T: std::clone::Clone + Send + Sync + 'static> {
    // orig_addr: usize,
    addr: usize,
    pe: usize,
    size: usize,
    backend: Backend,
    rdma: Arc<dyn LamellaeRDMA>,
    cnt: Arc<AtomicUsize>,
    local: bool,
    phantom: PhantomData<T>,
}

//#[prof]
impl<T: std::clone::Clone + Send + Sync + 'static> MemoryRegion for LamellarMemoryRegion<T> {
    fn id(&self) -> usize {
        self.addr //probably should be key
    }
}

//#[prof]
impl<T: std::clone::Clone + Send + Sync + 'static> From<__NetworkLamellarMemoryRegion<T>>
    for LamellarMemoryRegion<T>
{
    fn from(reg: __NetworkLamellarMemoryRegion<T>) -> Self {
        let temp = ACTIVE.get(reg.backend);
        temp.1.fetch_add(1, Ordering::SeqCst);
        let rdma = temp.0;
        let lmr = LamellarMemoryRegion {
            // orig_addr: rdma.local_addr(reg.pe, reg.orig_addr),
            addr: rdma.local_addr(reg.pe, reg.addr),
            pe: rdma.my_pe(),
            size: reg.size,
            backend: reg.backend,
            rdma: rdma,
            cnt: temp.1.clone(),
            local: reg.local,
            phantom: reg.phantom,
        };
        // println!("nlmr: addr {:x} pe: {:?} size: {:?} backend {:?}, lmr: addr: {:x} pe: {:?} size: {:?} backend {:?} cnt {:?}",reg.addr,reg.pe,reg.size,reg.backend,lmr.addr,lmr.pe,lmr.size,lmr.backend, lmr.cnt.load(Ordering::SeqCst));
        lmr
    }
}

//#[prof]
impl<T: std::clone::Clone + Send + Sync + 'static> LamellarMemoryRegion<T> {
    pub(crate) fn new(
        size: usize,
        lamellae: Arc<Lamellae>,
        alloc: AllocationType,
    ) -> LamellarMemoryRegion<T> {
        let cnt = Arc::new(AtomicUsize::new(1));
        ACTIVE.insert(lamellae.backend(), lamellae.clone(), cnt.clone());
        // let rdma = lamellae.clone();
        println!("creating new lamellar memory region {:?}",size * std::mem::size_of::<T>());
        let mut local = false;
        let addr = if let AllocationType::Local = alloc{
            local = true;
            lamellae.rt_alloc(size * std::mem::size_of::<T>()).unwrap() + lamellae.base_addr()
        } else {
            lamellae.alloc(size * std::mem::size_of::<T>(), alloc).unwrap()
        };
        let temp = LamellarMemoryRegion {
            addr: addr,
            pe: lamellae.my_pe(),
            size: size,
            backend: lamellae.backend(),
            rdma: lamellae,
            cnt: cnt,
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

    /// copy data from local memory location into a remote memory localtion
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    pub unsafe fn put(&self, pe: usize, index: usize, data: &impl RegisteredMemoryRegion) {
        //todo make return a result?
        if index + data.len() <= self.size {
            let num_bytes = data.len() * std::mem::size_of::<T>();
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

    pub(crate) unsafe fn put_slice(&self, pe: usize, index: usize, data: &[T]) {
        //todo make return a result?
        if index + data.len() <= self.size {
            let num_bytes = data.len() * std::mem::size_of::<T>();
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

    pub unsafe fn put_all(&self, index: usize, data: &impl RegisteredMemoryRegion) {
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
    pub unsafe fn get(&self, pe: usize, index: usize, data: &impl RegisteredMemoryRegion) {
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

    // pub fn as_slice(&self) -> &[T] {
    //     unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) }
    // }

    // pub unsafe fn as_mut_slice(&self) -> &mut [T] {
    //     std::slice::from_raw_parts_mut(self.addr as *mut T, self.size)
    // }

    pub unsafe fn as_base<
        B: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        self,
    ) -> LamellarMemoryRegion<B> {
        self.cnt.fetch_add(1, Ordering::SeqCst);
        LamellarMemoryRegion {
            addr: self.addr, //TODO: out of memory...
            pe: self.pe,
            size: self.size,
            backend: self.backend,
            rdma: self.rdma.clone(),
            cnt: self.cnt.clone(),
            local: self.local,
            phantom: PhantomData,
        }
    }
}

//#[prof]
impl<T: std::clone::Clone + Send + Sync + 'static> Clone for LamellarMemoryRegion<T> {
    fn clone(&self) -> Self {
        self.cnt.fetch_add(1, Ordering::SeqCst);
        let lmr = LamellarMemoryRegion {
            addr: self.addr,
            pe: self.pe,
            size: self.size,
            backend: self.backend,
            rdma: self.rdma.clone(),
            cnt: self.cnt.clone(),
            local: self.local,
            phantom: self.phantom,
        };
        // println!("clone: {:?}",lmr);
        // println!("nlmr: addr: {:?} size: {:?} backend {:?}, lmr: addr: {:?} size: {:?} backend {:?}",reg.addr,reg.size,reg.backend,lmr.addr,lmr.size,lmr.backend);
        lmr
    }
}

//#[prof]
impl<T: std::clone::Clone + Send + Sync + 'static> Drop for LamellarMemoryRegion<T> {
    fn drop(&mut self) {
        let cnt = self.cnt.fetch_sub(1, Ordering::SeqCst);
        // //println!("drop: {:?} {:?}",self,cnt);

        if cnt == 1 {
            ACTIVE.remove(self.backend);
            // println!("trying to dropping mem region {:?}",self);
            if self.local {
                self.rdma.rt_free(self.addr - self.rdma.base_addr()); // - self.rdma.base_addr());
            } else {
                self.rdma.free(self.addr);
            }
            // ACTIVE.print();
            //println!("dropping mem region {:?}",self);
        }
    }
}

//#[prof]
impl<T: std::clone::Clone + Send + Sync + 'static> std::fmt::Debug for LamellarMemoryRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
        // write!(f, "{:?}", slice)
        write!(
            f,
            "addr {:#x} size {:?} backend {:?} cnt: {:?}",
            self.addr,
            self.size,
            self.backend,
            self.cnt.load(Ordering::SeqCst)
        )
    }
}

//#[prof]
impl<T: std::clone::Clone + Send + Sync + 'static> std::fmt::Debug
    for __NetworkLamellarMemoryRegion<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
        // write!(f, "{:?}", slice)
        write!(
            f,
            "pe {:?} size {:?} backend {:?} ",
            self.pe, self.size, self.backend,
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct LamellarLocalMemoryRegion<T: std::clone::Clone + Send + Sync + 'static> {
    lmr: LamellarMemoryRegion<T>,
    pe: usize,
}

impl<T: std::clone::Clone + Send + Sync + 'static> LamellarLocalMemoryRegion<T> {
    pub(crate) fn new(
        size: usize,
        lamellae: Arc<Lamellae>,
    ) -> LamellarLocalMemoryRegion<T> {
        let lmr = LamellarMemoryRegion::new(size, lamellae, AllocationType::Local);
        let pe = lmr.pe;
        LamellarLocalMemoryRegion { lmr: lmr, pe: pe }
    }

    pub unsafe fn put(&self, index: usize, data: &impl RegisteredMemoryRegion) {
        self.lmr.put(self.pe, index, data)
    }

    pub unsafe fn get(&self, index: usize, data: &impl RegisteredMemoryRegion) {
        self.lmr.get(self.pe, index, data)
    }

    pub unsafe fn as_base<
        B: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        self,
    ) -> LamellarLocalMemoryRegion<B> {
        LamellarLocalMemoryRegion {
            lmr: self.lmr.as_base::<B>(),
            pe: self.pe,
        }
    }
}

impl<T: std::clone::Clone + Send + Sync + 'static> RegisteredMemoryRegion
    for LamellarLocalMemoryRegion<T>
{
    type Output = T;
    fn len(&self) -> usize {
        // if self.pe == self.lmr.pe {
            self.lmr.len()
        // } else {
            // 0
        // }
    }
    fn addr(&self) -> MemResult<usize> {
        if self.pe == self.lmr.pe {
            self.lmr.addr()
        } else {
            Err(MemNotLocalError {})
        }
    }

    fn as_slice(&self) -> MemResult<&[T]> {
        if self.pe == self.lmr.pe {
            self.lmr.as_slice()
        } else {
            Err(MemNotLocalError {})
        }
    }

    unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        if self.pe == self.lmr.pe {
            self.lmr.as_mut_slice()
        } else {
            Err(MemNotLocalError {})
        }
    }
    fn as_ptr(&self) -> MemResult<*const T> {
        if self.pe == self.lmr.pe {
            self.lmr.as_ptr()
        } else {
            Err(MemNotLocalError {})
        }
    }
    fn as_mut_ptr(&self) -> MemResult<*mut T> {
        if self.pe == self.lmr.pe {
            self.lmr.as_mut_ptr()
        } else {
            Err(MemNotLocalError {})
        }
    }

    fn sub_region<R: RangeBounds<usize>>(&self, range: R) -> LamellarLocalMemoryRegion<T> {
        let lmr = self.lmr.sub_region(range);
        LamellarLocalMemoryRegion {
            lmr: lmr,
            pe: self.pe,
        }
    }
}

impl<T: std::clone::Clone + Send + Sync + 'static> MemoryRegion for LamellarLocalMemoryRegion<T> {
    fn id(&self) -> usize {
        self.lmr.addr //probably should be key
    }
}

impl<
        T: std::clone::Clone
            + Send
            + Sync
            + 'static,
    > std::fmt::Debug for LamellarLocalMemoryRegion<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
        // write!(f, "{:?}", slice)
        write!(
            f,
            "[{:?}] local mem region:  {:?} ",self.pe,self.lmr,
        )
    }
}


