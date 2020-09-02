use crate::lamellae::{Backend, Lamellae,LamellaeRDMA};
use core::marker::PhantomData;

use std::hash::{Hash, Hasher};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize,Ordering};

lazy_static! {
    static ref ACTIVE: CountedHashMap =CountedHashMap::new();
}


struct CountedHashMap {
    lock: RwLock<CountedHashMapInner>,
}
// unsafe impl Send for CountedHashMap {}

struct CountedHashMapInner {
    cnts: HashMap<Backend, usize>,
    lamellaes: HashMap<Backend, (Arc<dyn Lamellae + Send + Sync>,Arc<AtomicUsize>)>,
}

impl CountedHashMap {
    pub fn new() -> CountedHashMap {
        CountedHashMap {
            lock: RwLock::new(CountedHashMapInner {
                cnts: HashMap::new(),
                lamellaes: HashMap::new(),
            }),
        }
    }

    pub fn insert(&self, backend: Backend, lamellae: Arc<dyn Lamellae + Send + Sync>, cnt: Arc<AtomicUsize>) {
        let mut map = self.lock.write();
        let mut insert = false;
        *map.cnts.entry(backend).or_insert_with(|| {
            insert = true;
            0
        }) += 1;
        if insert{
            map.lamellaes.insert(backend, (lamellae,cnt));
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

    pub fn get(&self, backend: Backend) -> (Arc<dyn Lamellae  + Send + Sync>,Arc<AtomicUsize>) {
        let map = self.lock.read();
        map.lamellaes.get(&backend).expect("invalid key").clone()
    }
}

pub trait RemoteMemoryRegion {
    /// allocate a remote memory region from the symmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    fn alloc_mem_region<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + std::marker::Sized
            + 'static,
    >(&self,
        size: usize,
    ) -> LamellarMemoryRegion<T>;

    /// release a remote memory region from the symmetric heap
    ///
    /// # Arguments
    ///
    /// * `region` - the region to free
    ///
    fn free_memory_region<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + std::fmt::Debug
            + 'static,
    >(&self,
        region: LamellarMemoryRegion<T>,
    );
}

pub(crate) trait MemoryRegion{
   fn id(&self) -> usize;
}

impl Hash for Box<dyn MemoryRegion> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl PartialEq  for Box<dyn MemoryRegion> {
    fn eq(&self, other: &Box<dyn MemoryRegion>) -> bool {
        self.id() == other.id()
    }
}

impl Eq for Box<dyn MemoryRegion> {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy)]
pub struct __NetworkLamellarMemoryRegion<
    T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static,
> {
    addr: usize,
    size: usize,
    backend: Backend,
    phantom: PhantomData<T>,
}

impl<
T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static,
> From<LamellarMemoryRegion<T>> for __NetworkLamellarMemoryRegion<T> {
    fn from(reg: LamellarMemoryRegion<T>) -> Self{
        let nlmr = __NetworkLamellarMemoryRegion{
            addr: reg.addr - ACTIVE.get(reg.backend).0.get_rdma().base_addr() ,
            size: reg.size,
            backend: reg.backend,
            phantom: reg.phantom
        };
        // println!("lmr: addr: {:?} size: {:?} backend {:?}, nlmr: addr: {:?} size: {:?} backend {:?}",reg.addr,reg.size,reg.backend,nlmr.addr,nlmr.size,nlmr.backend);
        nlmr
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(into = "__NetworkLamellarMemoryRegion<T>", from = "__NetworkLamellarMemoryRegion<T>")]
pub struct LamellarMemoryRegion<
    T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static,
> {
    addr: usize,
    size: usize,
    backend: Backend,
    // lamellae: Arc<dyn Lamellae + Sync + Send>,
    rdma: Arc<dyn LamellaeRDMA>,
    cnt: Arc<AtomicUsize>,
    phantom: PhantomData<T>,
}

impl<
T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static,
> MemoryRegion for LamellarMemoryRegion<T> {
    fn id(&self)-> usize{
        self.addr
    }
}

impl<
T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static,
> From<__NetworkLamellarMemoryRegion<T>> for LamellarMemoryRegion<T> {
    fn from(reg: __NetworkLamellarMemoryRegion<T>) -> Self{
        let temp = ACTIVE.get(reg.backend);
        temp.1.fetch_add(1,Ordering::SeqCst);
        let lmr = LamellarMemoryRegion{
            addr: reg.addr + temp.0.get_rdma().base_addr(),
            size: reg.size,
            backend: reg.backend,
            rdma: temp.0.get_rdma(),
            cnt: temp.1.clone(),
            phantom: reg.phantom
        };
        // println!("nlmr: addr: {:?} size: {:?} backend {:?}, lmr: addr: {:?} size: {:?} backend {:?}",reg.addr,reg.size,reg.backend,lmr.addr,lmr.size,lmr.backend);
        lmr
    }
}

impl<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    > LamellarMemoryRegion<T>
{
    pub(crate) fn new(size: usize, lamellae: Arc<dyn Lamellae + Sync + Send>) -> LamellarMemoryRegion<T> {
        let cnt = Arc::new(AtomicUsize::new(1));
        ACTIVE.insert(lamellae.backend(),lamellae.clone(),cnt.clone());
        let temp = LamellarMemoryRegion {
            addr: lamellae
                .get_rdma()
                .alloc(size * std::mem::size_of::<T>())
                .unwrap() + lamellae
                .get_rdma().base_addr(), //TODO: out of memory...
            size: size,
            backend: lamellae.backend(),
            rdma: lamellae.get_rdma(),
            cnt: cnt,
            phantom: PhantomData,
        };
        // println!("new memregion {:?}",temp);
        temp
    }

    // pub(crate) fn delete(self, lamellae: Arc<dyn Lamellae>) {
    //     assert!(
    //         lamellae.backend() == self.backend,
    //         "free mem region associated with wrong backend"
    //     );

    //     if ACTIVE.get(self.backend).1.load(Ordering::SeqCst) == 0{
    //         ACTIVE.remove(lamellae.backend());
    //         self.rdma.free(self.addr-self.rdma.base_addr());
    //     }
    // }
    //currently unsafe because user must ensure data exists utill put is finished
    pub unsafe fn put(&self, pe: usize, index: usize, data: &[T]) {
        if index + data.len() <= self.size {
            let num_bytes = data.len()*std::mem::size_of::<T>();
            let bytes = std::slice::from_raw_parts(data.as_ptr() as *const u8,num_bytes);
            // ACTIVE.get(self.backend).get_rdma().put(pe, bytes, self.addr + index * std::mem::size_of::<T>());
            // self.lamellae.get_rdma().put(pe, bytes, self.addr + index * std::mem::size_of::<T>());
            self.rdma.put(pe, bytes, self.addr + index * std::mem::size_of::<T>())
        } else {
            println!(
                "{:?} {:?} {:?}",
                self.size,
                index,
                std::mem::size_of_val(data)
            );
            panic!("index out of bounds");
        }
    }
    
    pub  fn iput(&self, pe: usize, index: usize, data: &[T]) {
        if index + data.len() <= self.size {
            let num_bytes = data.len()*std::mem::size_of::<T>();
            let bytes = unsafe {std::slice::from_raw_parts(data.as_ptr() as *const u8,num_bytes)};
            // ACTIVE.get(self.backend).get_rdma().put(pe, bytes, self.addr + index * std::mem::size_of::<T>());
            // self.lamellae.get_rdma().put(pe, bytes, self.addr + index * std::mem::size_of::<T>());
            // println!("[{:?}] put: {:?} {:?} {:?} {:?}",self.rdma.mype(),pe,self.addr,self.addr+index* std::mem::size_of::<T>(),bytes);

            self.rdma.iput(pe, bytes, self.addr + index * std::mem::size_of::<T>())
        } else {
            println!(
                "{:?} {:?} {:?}",
                self.size,
                index,
                std::mem::size_of_val(data)
            );
            panic!("index out of bounds");
        }
    }
    //currently unsafe because user must ensure data exists utill put is finished
    pub unsafe fn put_all(&self, index: usize, data: &[T]) {
        if index + data.len() <= self.size {
            let num_bytes = data.len()*std::mem::size_of::<T>();
            let bytes = std::slice::from_raw_parts(data.as_ptr() as *const u8,num_bytes);
            // ACTIVE.get(self.backend).get_rdma().put_all(bytes, self.addr + index * std::mem::size_of::<T>());
            // self.lamellae.get_rdma().put_all(bytes, self.addr + index * std::mem::size_of::<T>());
            self.rdma.put_all(bytes, self.addr + index * std::mem::size_of::<T>());
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
    /// * `dst` - address of destination buffer
    ///
    pub unsafe fn get(&self, pe: usize, index: usize, data: &mut [T]) {
        if index + data.len() <= self.size {
            let num_bytes = data.len()*std::mem::size_of::<T>();
            let bytes = std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut u8,num_bytes);
            // ACTIVE.get(self.backend).get_rdma().get(pe, self.addr + index * std::mem::size_of::<T>(), bytes); //(remote pe, src, dst)
            // self.lamellae.get_rdma().get(pe, self.addr + index * std::mem::size_of::<T>(), bytes); //(remote pe, src, dst)
            // println!("[{:?}] get: {:?} {:?} {:?} {:?}",self.rdma.mype(),pe,self.addr,self.addr+index* std::mem::size_of::<T>(),bytes);
            self.rdma.get(pe, self.addr + index * std::mem::size_of::<T>(), bytes); //(remote pe, src, dst)
        } else {
            println!(
                "{:?} {:?} {:?} {:?}",
                self.size,
                index,
                data.len(),
                std::mem::size_of_val(data)
            );
            panic!("index out of bounds");
        }
    }

    // pub fn as_bytes(&self) -> &[u8] {
    //     let pointer = self as *const T as *const u8;
    //     let size = std::mem::size_of::<T>();
    //     let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
    //     slice
    // }
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) }
    }

    pub unsafe fn as_mut_slice(&self) -> &mut [T] {
        std::slice::from_raw_parts_mut(self.addr as *mut T, self.size)
    }
}

impl<
T:    serde::ser::Serialize
    + serde::de::DeserializeOwned
    + std::clone::Clone
    + Send
    + Sync
    + 'static,
> Clone for LamellarMemoryRegion<T> {
    fn clone(&self) -> Self {
        self.cnt.fetch_add(1,Ordering::SeqCst);
        let lmr = LamellarMemoryRegion{
            addr: self.addr,
            size: self.size,
            backend: self.backend,
            rdma: self.rdma.clone(),
            cnt: self.cnt.clone(),
            phantom: self.phantom
        };
        // println!("nlmr: addr: {:?} size: {:?} backend {:?}, lmr: addr: {:?} size: {:?} backend {:?}",reg.addr,reg.size,reg.backend,lmr.addr,lmr.size,lmr.backend);
        lmr
    }
}

impl<
T:    serde::ser::Serialize
    + serde::de::DeserializeOwned
    + std::clone::Clone
    + Send
    + Sync
    + 'static,
> Drop for LamellarMemoryRegion<T> {
    fn drop(&mut self) {
        let cnt = self.cnt.fetch_sub(1,Ordering::SeqCst);
        if cnt == 1{
            ACTIVE.remove(self.backend);
            // println!("trying to dropping mem region {:?}",self);
            self.rdma.free(self.addr-self.rdma.base_addr());
            // println!("dropping mem region {:?}",self);
        }
    }
}

impl<
        T:  serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    > std::fmt::Debug for LamellarMemoryRegion<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
        // write!(f, "{:?}", slice)
        write!(f,"addr {:?} size {:?} backend {:?}",self.addr,self.size,self.backend)
    }
}

impl<
        T:  serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    > std::fmt::Debug for __NetworkLamellarMemoryRegion<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
        // write!(f, "{:?}", slice)
        write!(f,"addr {:?} size {:?} backend {:?}",self.addr,self.size,self.backend)
    }
}
