use std::{
    ffi::c_void,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use ucx1_sys::*;

use super::{context::Context, endpoint::Endpoint, error::Error};
use pmi::{pmi::Pmi, pmix::PmiX};

pub struct MemoryHandle {
    pub(crate) inner: Arc<MemoryHandleInner>,
    pub(crate) addr: usize,
    pub(crate) size: usize,
}

static MEMREGION_CNT: AtomicUsize = AtomicUsize::new(0);

impl MemoryHandle {
    pub fn as_slice<T>(&self) -> &[T] {
        unsafe {
            std::slice::from_raw_parts(self.addr as *const T, self.size / std::mem::size_of::<T>())
        }
    }

    pub fn as_mut_slice<T>(&self) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.addr as *mut T,
                self.size / std::mem::size_of::<T>(),
            )
        }
    }

    pub fn sub_alloc(&self, offset: usize, size: usize) -> Self {
        assert!(offset + size <= self.size);
        MemoryHandle {
            inner: self.inner.clone(),
            addr: self.addr + offset,
            size,
        }
    }
}

/// A memory region allocated through UCP library,
/// which is optimized for remote memory access operations.
#[derive(Debug)]
pub struct MemoryHandleInner {
    handle: ucp_mem_h,
    pub(crate) addr: usize,
    context: Arc<Context>,
}

unsafe impl Send for MemoryHandleInner {}
unsafe impl Sync for MemoryHandleInner {}

impl PartialEq for MemoryHandleInner {
    fn eq(&self, other: &Self) -> bool {
        self.context.handle == other.context.handle
            && self.addr == other.addr
            && self.handle == other.handle
    }
}

impl Eq for MemoryHandleInner {}
impl std::hash::Hash for MemoryHandleInner {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.context.handle.hash(state);
        self.addr.hash(state);
        self.handle.hash(state);
    }
}

impl MemoryHandleInner {
    pub(crate) fn alloc(context: &Arc<Context>, size: usize) -> Arc<Self> {
        let params = ucp_mem_map_params_t {
            field_mask: (ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_LENGTH
                | ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_FLAGS)
                .0 as u64,
            address: std::ptr::null_mut(),
            length: (size) as _,
            flags: UCP_MEM_MAP_ALLOCATE as _,
            prot: 0,
            memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
            exported_memh_buffer: std::ptr::null_mut(),
        };
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_mem_map(context.handle, &params, handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let handle = unsafe { handle.assume_init() };
        // println!("handle: {:?}", handle);
        let mut attr = ucp_mem_attr_t {
            field_mask: (ucp_mem_attr_field::UCP_MEM_ATTR_FIELD_ADDRESS
                | ucp_mem_attr_field::UCP_MEM_ATTR_FIELD_LENGTH)
                .0 as u64,
            length: size as _,
            address: std::ptr::null_mut(),
            mem_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
        };
        let status = unsafe { ucp_mem_query(handle, &mut attr) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        // println!("attr: {:?} {}", attr.address, attr.length);

        Arc::new(MemoryHandleInner {
            handle,
            addr: attr.address as _,
            context: context.clone(),
        })
    }

    /// Register memory region.
    pub(crate) fn register<T>(context: &Arc<Context>, region: &mut [T]) -> Arc<Self> {
        #[allow(invalid_value)]
        #[allow(clippy::uninit_assumed_init)]
        let params = ucp_mem_map_params_t {
            field_mask: (ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_ADDRESS
                | ucp_mem_map_params_field::UCP_MEM_MAP_PARAM_FIELD_LENGTH)
                .0 as u64,
            address: region.as_ptr() as _,
            length: (region.len() * std::mem::size_of::<T>()) as _,
            flags: 0 as _,
            prot: 0 as _,
            memory_type: ucs_memory_type::UCS_MEMORY_TYPE_HOST,
            exported_memh_buffer: std::ptr::null_mut(),
        };
        let mut handle = MaybeUninit::uninit();
        let status = unsafe { ucp_mem_map(context.handle, &params, handle.as_mut_ptr()) };
        assert_eq!(status, ucs_status_t::UCS_OK);
        Arc::new(MemoryHandleInner {
            handle: unsafe { handle.assume_init() },
            addr: region.as_ptr() as usize,
            context: context.clone(),
        })
    }

    /// Packs into the buffer a remote access key (RKEY) object.
    pub(crate) fn pack(&self) -> RKeyBuffer {
        let mut buf = MaybeUninit::uninit();
        let mut len = MaybeUninit::uninit();
        let status = unsafe {
            ucp_rkey_pack(
                self.context.handle,
                self.handle,
                buf.as_mut_ptr(),
                len.as_mut_ptr(),
            )
        };
        assert_eq!(status, ucs_status_t::UCS_OK);
        RKeyBuffer {
            buf: unsafe { buf.assume_init() },
            len: unsafe { len.assume_init() },
        }
    }

    //TODO create an exchange that uses a single registered memory region

    pub(crate) fn exchange_key(
        &self,
        endpoints: &[Arc<Endpoint>],
        pmi: &Arc<PmiX>,
    ) -> Result<Vec<(usize, Arc<RKey>)>, Error> {
        let rkey = self.pack();
        let mut address_and_key = self.addr.to_ne_bytes().to_vec();
        // println!("[exchange_key] address: {:x?}", address_and_key);
        // println!("[exchange_key] key: {:x?}", rkey.as_ref());
        address_and_key.extend_from_slice(rkey.as_ref());
        // println!("[exchange_key] address_and_key: {:x?}", address_and_key);
        let id = format!(
            "mem_region_address_{}",
            MEMREGION_CNT.fetch_add(1, Ordering::SeqCst)
        );
        pmi.put(&id, &address_and_key).unwrap();
        pmi.exchange().unwrap();

        let mut all_rkeys = Vec::new();
        for pe in 0..pmi.ranks().len() {
            let res = pmi.get(&id, &address_and_key.len(), &pe).unwrap();
            // println!("[exchange_key] {pe}: remote address_and_key {:x?}", res);
            let remote_address = usize::from_ne_bytes(res[0..8].try_into().unwrap());
            // println!("[exchange_key] {pe}: remote_address: {:x}", remote_address);
            let remote_rkey = RKey::unpack(&endpoints[pe], &res[8..]);
            // println!("[exchange_key] {pe}: remote_rkey: {:?}", remote_rkey);
            all_rkeys.push((remote_address, Arc::new(remote_rkey)));
        }
        Ok(all_rkeys)
    }
}

impl Drop for MemoryHandleInner {
    fn drop(&mut self) {
        // println!("dropping MemoryHandleInner {:x}", self.addr);
        unsafe { ucp_mem_unmap(self.context.handle, self.handle) };
    }
}

/// An owned buffer containing remote access key.
#[derive(Debug)]
pub struct RKeyBuffer {
    buf: *mut c_void,
    len: usize,
}

impl AsRef<[u8]> for RKeyBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buf as _, self.len as _) }
    }
}

impl Drop for RKeyBuffer {
    fn drop(&mut self) {
        unsafe { ucp_rkey_buffer_release(self.buf as _) }
    }
}

/// Remote access key.
#[derive(Debug)]
pub struct RKey {
    pub(crate) handle: ucp_rkey_h,
}

unsafe impl Send for RKey {}
unsafe impl Sync for RKey {}

impl RKey {
    /// Create remote access key from packed buffer.
    pub fn unpack(endpoint: &Endpoint, rkey_buffer: &[u8]) -> Self {
        let mut handle = MaybeUninit::uninit();
        let status = unsafe {
            ucp_ep_rkey_unpack(
                endpoint.handle,
                rkey_buffer.as_ptr() as _,
                handle.as_mut_ptr(),
            )
        };
        assert_eq!(status, ucs_status_t::UCS_OK);
        // println!("unpacked rkey: {:?}", unsafe { handle.assume_init() });
        RKey {
            handle: unsafe { handle.assume_init() },
        }
    }
}

impl Drop for RKey {
    fn drop(&mut self) {
        unsafe { ucp_rkey_destroy(self.handle) }
    }
}
