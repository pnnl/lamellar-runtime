use crate::array::{LamellarArrayInput, LamellarRead, LamellarWrite, MyFrom};
use crate::lamellae::{AllocationType, Backend, Lamellae, LamellaeComm, LamellaeRDMA};
use crate::lamellar_team::LamellarTeamRT;
use core::marker::PhantomData;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

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

pub trait Dist2: Send + Sync + Copy {}
impl<T: Send + Sync + Copy> Dist2 for T {}

pub trait Dist: Send + Sync + Copy + 'static
{
}
// impl<T: Send + Sync + Copy /*+ 'static*/>
//     Dist for T
// {
// }

#[enum_dispatch(RegisteredMemoryRegion<T>, MemRegionId, AsBase, SubRegion<T>, MemoryRegionRDMA<T>, RTMemoryRegionRDMA<T>)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum LamellarMemoryRegion<T: Dist> {
    Shared(SharedMemoryRegion<T>),
    Local(LocalMemoryRegion<T>),
}

impl<T: Dist> crate::DarcSerde for LamellarMemoryRegion<T> {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared ser");
        match self {
            LamellarMemoryRegion::Shared(mr) => mr.ser(num_pes,cur_pe),
            LamellarMemoryRegion::Local(mr) => mr.ser(num_pes,cur_pe),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match self {
            LamellarMemoryRegion::Shared(mr) => mr.des(cur_pe),
            LamellarMemoryRegion::Local(mr) => mr.des(cur_pe),
        }
        // self.mr.print();
    }
}

impl<T: Dist> LamellarMemoryRegion<T> {
    pub unsafe fn as_mut_slice(&self) -> MemResult<&mut [T]> {
        match self {
            LamellarMemoryRegion::Shared(memregion) => memregion.as_mut_slice(),
            LamellarMemoryRegion::Local(memregion) => memregion.as_mut_slice(),
        }
    }

    pub unsafe fn as_slice(&self) -> MemResult<&[T]> {
        match self {
            LamellarMemoryRegion::Shared(memregion) => memregion.as_slice(),
            LamellarMemoryRegion::Local(memregion) => memregion.as_slice(),
        }
    }

    pub fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarMemoryRegion<T> {
        match self {
            LamellarMemoryRegion::Shared(memregion) => memregion.sub_region(range).into(),
            LamellarMemoryRegion::Local(memregion) => memregion.sub_region(range).into(),
        }
    }
}

impl<T: Dist> From<&LamellarMemoryRegion<T>> for LamellarArrayInput<T> {
    fn from(mr: &LamellarMemoryRegion<T>) -> Self {
        LamellarArrayInput::LamellarMemRegion(mr.clone())
        // match mr.clone(){
        //     LamellarMemoryRegion::Shared(mr) => LamellarArrayInput::SharedMemRegion(mr),
        //     LamellarMemoryRegion::Local(mr) => LamellarArrayInput::LocalMemRegion(mr),
        // }
    }
}

impl<T: Dist> MyFrom<&LamellarMemoryRegion<T>> for LamellarArrayInput<T> {
    fn my_from(mr: &LamellarMemoryRegion<T>, _team: &std::pin::Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarArrayInput::LamellarMemRegion(mr.clone())
        // match mr.clone(){
        //     LamellarMemoryRegion::Shared(mr) => LamellarArrayInput::SharedMemRegion(mr),
        //     LamellarMemoryRegion::Local(mr) => LamellarArrayInput::LocalMemRegion(mr),
        // }
    }
}

// impl<T: Dist> From<LamellarMemoryRegion<T>> for LamellarArrayInput<T> {
//     fn from(mr: LamellarMemoryRegion<T>) -> Self {
//         LamellarArrayInput::LamellarMemRegion(mr)
//     }
// }

impl<T: Dist> MyFrom<LamellarMemoryRegion<T>> for LamellarArrayInput<T> {
    fn my_from(mr: LamellarMemoryRegion<T>, _team: &std::pin::Pin<Arc<LamellarTeamRT>>) -> Self {
        LamellarArrayInput::LamellarMemRegion(mr)
        // match mr{
        //     LamellarMemoryRegion::Shared(mr) => LamellarArrayInput::SharedMemRegion(mr),
        //     LamellarMemoryRegion::Local(mr) => LamellarArrayInput::LocalMemRegion(mr),
        // }
    }
}

#[enum_dispatch]
pub trait RegisteredMemoryRegion<T: Dist> {
    fn len(&self) -> usize;
    fn addr(&self) -> MemResult<usize>;
    fn as_slice(&self) -> MemResult<&[T]>;
    fn at(&self, index: usize) -> MemResult<&T>;
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
pub trait SubRegion<T: Dist> {
    fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> LamellarMemoryRegion<T>;
}

#[enum_dispatch]
pub(crate) trait AsBase {
    unsafe fn to_base<B: Dist>(self) -> LamellarMemoryRegion<B>;
}

#[enum_dispatch]
pub trait MemoryRegionRDMA<T: Dist> {
    unsafe fn put<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U);
    fn iput<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U);
    unsafe fn put_all<U: Into<LamellarMemoryRegion<T>>>(&self, index: usize, data: U);
    unsafe fn get_unchecked<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    );
    fn iget<U: Into<LamellarMemoryRegion<T>>>(&self, pe: usize, index: usize, data: U);
}

#[enum_dispatch]
pub(crate) trait RTMemoryRegionRDMA<T: Dist> {
    unsafe fn put_slice(&self, pe: usize, index: usize, data: &[T]);
    unsafe fn iget_slice(&self, pe: usize, index: usize, data: &mut [T]);
}

//#[prof]
impl<T: Dist> Hash for LamellarMemoryRegion<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

//#[prof]
impl<T: Dist> PartialEq for LamellarMemoryRegion<T> {
    fn eq(&self, other: &LamellarMemoryRegion<T>) -> bool {
        self.id() == other.id()
    }
}

//#[prof]
impl<T: Dist> Eq for LamellarMemoryRegion<T> {}

impl<T: Dist> LamellarWrite for LamellarMemoryRegion<T> {}
impl<T: Dist> LamellarRead for LamellarMemoryRegion<T> {}
impl<T: Dist> LamellarRead for &LamellarMemoryRegion<T> {}

#[derive(Copy, Clone)]
pub(crate) enum Mode {
    Local,
    Remote,
    Shared,
}

// this is not intended to be accessed directly by a user
// it will be wrapped in either a shared region or local region
// in shared regions its wrapped in a darc which allows us to send
// to different nodes, in local its wrapped in Arc (we dont currently support sending to other nodes)
// for local we would probably need to develop something like a one-sided initiated darc...
pub(crate) struct MemoryRegion<T: Dist> {
    addr: usize,
    pe: usize,
    size: usize,
    num_bytes: usize,
    backend: Backend,
    rdma: Arc<dyn LamellaeRDMA>,
    mode: Mode,
    phantom: PhantomData<T>,
}

impl<T: Dist> MemoryRegion<T> {
    pub(crate) fn new(
        size: usize, //number of elements of type T
        lamellae: Arc<Lamellae>,
        alloc: AllocationType,
    ) -> MemoryRegion<T> {
        if let Ok(memreg) = MemoryRegion::try_new(size, lamellae, alloc) {
            memreg
        } else {
            panic!("out of memory")
        }
    }
    pub(crate) fn try_new(
        size: usize, //number of elements of type T
        lamellae: Arc<Lamellae>,
        alloc: AllocationType,
    ) -> Result<MemoryRegion<T>, anyhow::Error> {
        // println!("creating new lamellar memory region {:?}",size * std::mem::size_of::<T>());
        let mut mode = Mode::Shared;
        let addr = if size > 0 {
            if let AllocationType::Local = alloc {
                mode = Mode::Local;
                lamellae.rt_alloc(size * std::mem::size_of::<T>())?
            } else {
                lamellae.alloc(size * std::mem::size_of::<T>(), alloc)? //did we call team barrer before this?
            }
        } else {
            return Err(anyhow::anyhow!("cant have negative sized memregion"));
        };
        let temp = MemoryRegion {
            addr: addr,
            pe: lamellae.my_pe(),
            size: size,
            num_bytes: size * std::mem::size_of::<T>(),
            backend: lamellae.backend(),
            rdma: lamellae,
            mode: mode,
            phantom: PhantomData,
        };
        // println!(
        //     "new memregion {:x} {:x}",
        //     temp.addr,
        //     size * std::mem::size_of::<T>()
        // );
        Ok(temp)
    }
    pub(crate) fn from_remote_addr(
        addr: usize,
        pe: usize,
        size: usize,
        lamellae: Arc<Lamellae>,
    ) -> Result<MemoryRegion<T>, anyhow::Error> {
        Ok(MemoryRegion {
            addr: addr,
            pe: pe,
            size: size,
            num_bytes: size * std::mem::size_of::<T>(),
            backend: lamellae.backend(),
            rdma: lamellae,
            mode: Mode::Remote,
            phantom: PhantomData,
        })
    }

    #[allow(dead_code)]
    pub(crate) unsafe fn to_base<B: Dist>(self) -> MemoryRegion<B> {
        //this is allowed as we consume the old object..
        assert_eq!(
            self.num_bytes % std::mem::size_of::<B>(),
            0,
            "Error converting memregion to new base, does not align"
        );
        MemoryRegion {
            addr: self.addr, //TODO: out of memory...
            pe: self.pe,
            size: self.num_bytes / std::mem::size_of::<B>(),
            num_bytes: self.num_bytes,
            backend: self.backend,
            rdma: self.rdma.clone(),
            mode: self.mode,
            phantom: PhantomData,
        }
    }

    // }

    //#[prof]
    // impl<T: AmDist+ 'static> MemoryRegionRDMA<T> for MemoryRegion<T> {
    /// copy data from local memory location into a remote memory location
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of local input buffer that will be put into the remote memory
    /// the data buffer may not be safe to upon return from this call, currently the user is responsible for completion detection,
    /// or you may use the similar iput call (with a potential performance penalty);
    pub(crate) unsafe fn put<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        //todo make return a result?
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_ptr() {
                let bytes = std::slice::from_raw_parts(ptr as *const u8, num_bytes);
                self.rdma
                    .put(pe, bytes, self.addr + index * std::mem::size_of::<R>())
            } else {
                panic!("ERROR: put data src is not local");
            }
        } else {
            println!(
                "mem region bytes: {:?} sizeof elem {:?} len {:?}",
                self.num_bytes,
                std::mem::size_of::<T>(),
                self.size
            );
            println!(
                "data bytes: {:?} sizeof elem {:?} len {:?} index: {:?}",
                data.len() * std::mem::size_of::<R>(),
                std::mem::size_of::<R>(),
                data.len(),
                index
            );
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
    pub(crate) fn iput<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        //todo make return a result?
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_ptr() {
                let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, num_bytes) };
                self.rdma
                    .iput(pe, bytes, self.addr + index * std::mem::size_of::<R>())
            } else {
                panic!("ERROR: put data src is not local");
            }
        } else {
            println!("{:?} {:?} {:?}", self.size, index, data.len());
            panic!("index out of bounds");
        }
    }

    pub(crate) unsafe fn put_all<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        index: usize,
        data: U,
    ) {
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_ptr() {
                let bytes = std::slice::from_raw_parts(ptr as *const u8, num_bytes);
                self.rdma
                    .put_all(bytes, self.addr + index * std::mem::size_of::<R>());
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
    pub(crate) unsafe fn get_unchecked<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_mut_ptr() {
                let bytes = std::slice::from_raw_parts_mut(ptr as *mut u8, num_bytes);
                // println!("getting {:?} {:?} {:?} {:?} {:?} {:?} {:?}",pe,index,std::mem::size_of::<R>(),data.len(), num_bytes,self.size, self.num_bytes);
                self.rdma
                    .get(pe, self.addr + index * std::mem::size_of::<R>(), bytes);
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

    /// copy data from remote memory location into provided data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of destination buffer to store result of the get
    ///    data will be present within the buffer once this returns.
    pub(crate) fn iget<R: Dist, U: Into<LamellarMemoryRegion<R>>>(
        &self,
        pe: usize,
        index: usize,
        data: U,
    ) {
        let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            if let Ok(ptr) = data.as_mut_ptr() {
                let bytes = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, num_bytes) };
                // println!("getting {:?} {:?} {:?} {:?} {:?} {:?} {:?}",pe,index,std::mem::size_of::<R>(),data.len(), num_bytes,self.size, self.num_bytes);
                self.rdma
                    .iget(pe, self.addr + index * std::mem::size_of::<R>(), bytes);
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

    //we must ensure the the slice will live long enough and that it already exsists in registered memory
    pub(crate) unsafe fn put_slice<R: Dist>(&self, pe: usize, index: usize, data: &[R]) {
        //todo make return a result?
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
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
                .put(pe, bytes, self.addr + index * std::mem::size_of::<R>())
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
    /// copy data from remote memory location into provided data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `data` - address (which is "registered" with network device) of destination buffer to store result of the get
    ///    data will be present within the buffer once this returns.
    pub(crate) fn iget_slice<R: Dist>(&self, pe: usize, index: usize, data: &mut [R]) {
        // let data = data.into();
        if (index + data.len()) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = data.len() * std::mem::size_of::<R>();
            let bytes =
                unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut u8, num_bytes) };
            // println!("getting {:?} {:?} {:?} {:?} {:?} {:?} {:?}",pe,index,std::mem::size_of::<R>(),data.len(), num_bytes,self.size, self.num_bytes);
    
            self.rdma
                .iget(pe, self.addr + index * std::mem::size_of::<R>(), bytes);
            //(remote pe, src, dst)
            // println!("getting {:?} {:?} [{:?}] {:?} {:?} {:?}",pe,self.addr + index * std::mem::size_of::<T>(),index,data.addr(),data.len(),num_bytes);
        } else {
            println!("{:?} {:?} {:?}", self.size, index, data.len(),);
            panic!("index out of bounds");
        }
    }

    #[allow(dead_code)]
    pub(crate) unsafe fn fill_from_remote_addr<R: Dist>(
        &self,
        my_index: usize,
        pe: usize,
        addr: usize,
        len: usize,
    ) {
        if (my_index + len) * std::mem::size_of::<R>() <= self.num_bytes {
            let num_bytes = len * std::mem::size_of::<R>();
            let my_offset = self.addr + my_index * std::mem::size_of::<R>();
            let bytes = std::slice::from_raw_parts_mut(my_offset as *mut u8, num_bytes);
            let local_addr = self.rdma.local_addr(pe, addr);
            self.rdma.iget(pe, local_addr, bytes);
        } else {
            println!(
                "mem region len: {:?} index: {:?} data len{:?}",
                self.size, my_index, len
            );
            panic!("index out of bounds");
        }
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.size
    }

    pub(crate) fn addr(&self) -> MemResult<usize> {
        Ok(self.addr)
    }

    pub(crate) fn casted_at<R: Dist>(&self, index: usize) -> MemResult<&R> {
        if self.addr != 0 {
            let num_bytes = self.size * std::mem::size_of::<T>();
            assert_eq!(
                num_bytes % std::mem::size_of::<R>(),
                0,
                "Error converting memregion to new base, does not align"
            );
            Ok(unsafe {
                &std::slice::from_raw_parts(
                    self.addr as *const R,
                    num_bytes / std::mem::size_of::<R>(),
                )[index]
            })
        } else {
            Err(MemNotLocalError {})
        }
    }

    pub(crate) fn as_slice(&self) -> MemResult<&[T]> {
        if self.addr != 0 {
            Ok(unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) })
        } else {
            Ok(&[])
        }
    }
    pub(crate) fn as_casted_slice<R: Dist>(&self) -> MemResult<&[R]> {
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
    pub(crate) unsafe fn as_casted_mut_slice<R: Dist>(&self) -> MemResult<&mut [R]> {
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
    #[allow(dead_code)]
    pub(crate) fn as_ptr(&self) -> MemResult<*const T> {
        Ok(self.addr as *const T)
    }
    #[allow(dead_code)]
    pub(crate) fn as_casted_ptr<R: Dist>(&self) -> MemResult<*const R> {
        Ok(self.addr as *const R)
    }
    #[allow(dead_code)]
    pub(crate) fn as_mut_ptr(&self) -> MemResult<*mut T> {
        Ok(self.addr as *mut T)
    }
    #[allow(dead_code)]
    pub(crate) fn as_casted_mut_ptr<R: Dist>(&self) -> MemResult<*mut R> {
        Ok(self.addr as *mut R)
    }
}

impl<T: Dist> MemRegionId for MemoryRegion<T> {
    fn id(&self) -> usize {
        self.addr //probably should be key
    }
}

pub trait RemoteMemoryRegion {
    /// allocate a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    fn alloc_shared_mem_region<T: Dist + std::marker::Sized>(
        &self,
        size: usize,
    ) -> SharedMemoryRegion<T>;

    /// allocate a shared memory region from the asymmetric heap
    ///
    /// # Arguments
    ///
    /// * `size` - number of elements of T to allocate a memory region for -- (not size in bytes)
    ///
    fn alloc_local_mem_region<T: Dist + std::marker::Sized>(
        &self,
        size: usize,
    ) -> LocalMemoryRegion<T>;

    // /// release a shared memory region from the asymmetric heap
    // ///
    // /// # Arguments
    // ///
    // /// * `region` - the region to free
    // ///
    // fn free_shared_memory_region<T: AmDist+ 'static>(&self, region: SharedMemoryRegion<T>);

    // /// release a shared memory region from the asymmetric heap
    // ///
    // /// # Arguments
    // ///
    // /// * `region` - the region to free
    // ///
    // fn free_local_memory_region<T: AmDist+ 'static>(&self, region: LocalMemoryRegion<T>);
}

impl<T: Dist> Drop for MemoryRegion<T> {
    fn drop(&mut self) {
        // println!("trying to dropping mem region {:?}",self);
        if self.addr != 0 {
            match self.mode {
                Mode::Local => self.rdma.rt_free(self.addr), // - self.rdma.base_addr());
                Mode::Shared => self.rdma.free(self.addr),
                Mode::Remote => {}
            }
        }
        // println!("dropping mem region {:?}",self);
    }
}

// #[prof]
impl<T: Dist> std::fmt::Debug for MemoryRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
