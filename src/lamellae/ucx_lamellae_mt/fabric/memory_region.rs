use std::{
    ffi::c_void,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use super::{context::Context, endpoint::Endpoint, error::Error, UcxMtAlloc};
use lamellar_ucx_sys::*;
use pmi::{pmi::Pmi, pmix::PmiX};
use std::vec::Vec;

#[derive(Debug, Clone)]
pub(crate) struct MemoryHandle {
    pub(crate) inner: Arc<MemoryHandleInner>,
    pub(crate) addr: usize,
    pub(crate) size: usize,
}

static MEMREGION_CNT: AtomicUsize = AtomicUsize::new(0);

impl MemoryHandle {
    pub(crate) fn as_mut_slice<T>(&self) -> &mut [T] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.addr as *mut T,
                self.size / std::mem::size_of::<T>(),
            )
        }
    }

    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> Self {
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
pub(crate) struct MemoryHandleInner {
    pub(crate) handle: ucp_mem_h,
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
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.addr as *const u8
    }
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

        Arc::new(MemoryHandleInner {
            handle,
            addr: attr.address as _,
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

    pub(crate) fn exchange_key_pmi(
        &self,
        _endpoints: &[Arc<Endpoint>],
        pmi: &Arc<PmiX>,
        slots: usize,
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
        // println!("[exchange_key_pmi] len: {}", address_and_key.len());
        pmi.put(&id, &address_and_key).unwrap();
        pmi.exchange().unwrap();

        let mut all_rkeys = Vec::new();
        for pe in 0..pmi.ranks().len() {
            let res = pmi.get(&id, &address_and_key.len(), &pe).unwrap();
            // println!("[exchange_key] {pe}: remote address_and_key {:x?}", res);
            let remote_address = usize::from_ne_bytes(res[0..8].try_into().unwrap());
            // println!("[exchange_key] {pe}: remote_address: {:x}", remote_address);
            let packed = res[8..].to_vec();
            let remote_rkey = RKey::from_packed_with_slots(packed, slots);
            all_rkeys.push((remote_address, Arc::new(remote_rkey)));
        }
        Ok(all_rkeys)
    }

    pub(crate) fn exchange_key_alloc(
        &self,
        _endpoints: &[Arc<Endpoint>],
        pmi: &Arc<PmiX>,
        exchange_buffer: &UcxMtAlloc,
        slots: usize,
    ) -> Result<Vec<(usize, Arc<RKey>)>, Error> {
        let rkey = self.pack();
        let mut address_and_key = self.addr.to_ne_bytes().to_vec();
        address_and_key.extend_from_slice(rkey.as_ref());
        // let id = format!(
        //     "mem_region_address_{}",
        //     MEMREGION_CNT.fetch_add(1, Ordering::SeqCst)
        // );
        // println!("[exchange_key_alloc] len: {}", address_and_key.len());

        pmi.barrier(false).expect("PMI Barrier failed");
        for pe in 0..exchange_buffer.num_pes {
            unsafe {
                exchange_buffer.put_inner(
                    pe,
                    exchange_buffer.my_pe * address_and_key.len(),
                    &address_and_key,
                    false,
                    false,
                )
            };
        }

        exchange_buffer.wait_all();
        pmi.barrier(false).expect("PMI Barrier failed");
        let ex_buff_slice = exchange_buffer.as_mut_slice::<u8>();
        // println!("[exchange_key_alloc] ex_buff size: {}", ex_buff_slice.len());
        let mut all_rkeys = Vec::new();
        for pe in 0..exchange_buffer.num_pes {
            let res = ex_buff_slice[pe * address_and_key.len()..(pe + 1) * address_and_key.len()]
                .to_vec();
            // println!("[exchange_key] {pe}: remote address_and_key {:x?}", res);
            let remote_address = usize::from_ne_bytes(res[0..8].try_into().unwrap());
            // println!("[exchange_key] {pe}: remote_address: {:x}", remote_address);
            let packed = res[8..].to_vec();
            let remote_rkey = RKey::from_packed_with_slots(packed, slots);
            all_rkeys.push((remote_address, Arc::new(remote_rkey)));
        }
        ex_buff_slice.fill(0);
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
pub(crate) struct RKeyBuffer {
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
pub(crate) struct RKey {
    // packed rkey bytes from ucp_rkey_pack
    packed: Vec<u8>,
    // per-comm-group unpacked handles cached in slots indexed by LAMELLAR_THREAD_ID % slots
    handles: Vec<AtomicUsize>,
}

unsafe impl Send for RKey {}
unsafe impl Sync for RKey {}

impl RKey {
    /// Create RKey from packed buffer (do not unpack yet).
    /// Create RKey from packed buffer and allocate `slots` cache entries.
    pub(crate) fn from_packed_with_slots(rkey_buffer: Vec<u8>, slots: usize) -> Self {
        let mut handles = Vec::with_capacity(slots);
        for _ in 0..slots {
            handles.push(AtomicUsize::new(0));
        }
        RKey {
            packed: rkey_buffer,
            handles,
        }
    }

    /// Get or create an unpacked rkey handle for `endpoint`.
    /// This is safe to call concurrently; unpacking for the same endpoint
    /// is performed once and cached.
    pub(crate) fn handle_for_endpoint(&self, endpoint: &Endpoint) -> ucp_rkey_h {
        use crate::LAMELLAR_THREAD_ID;
        let slots = self.handles.len();
        let idx = LAMELLAR_THREAD_ID.with(|id| *id) % slots;
        let cur = self.handles[idx].load(Ordering::Acquire);
        if cur != 0 {
            return cur as ucp_rkey_h;
        }
        // Unpack for this endpoint into a new handle
        let mut handle = MaybeUninit::uninit();
        let status = unsafe {
            ucp_ep_rkey_unpack(
                endpoint.handle,
                self.packed.as_ptr() as _,
                handle.as_mut_ptr(),
            )
        };
        assert_eq!(status, ucs_status_t::UCS_OK);
        let handle = unsafe { handle.assume_init() } as usize;
        // Attempt to publish our handle; if another thread already installed one, destroy ours and use theirs
        match self.handles[idx].compare_exchange(0, handle, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => handle as ucp_rkey_h,
            Err(existing) => {
                // another thread beat us; destroy our handle and use existing
                unsafe { ucp_rkey_destroy(handle as ucp_rkey_h) };
                existing as ucp_rkey_h
            }
        }
    }

    /// Expose packed bytes for exchange debug/use.
    pub(crate) fn as_packed_slice(&self) -> &[u8] {
        &self.packed
    }
}

impl Drop for RKey {
    fn drop(&mut self) {
        for h in &self.handles {
            let v = h.load(Ordering::Acquire);
            if v != 0 {
                unsafe { ucp_rkey_destroy(v as ucp_rkey_h) }
            }
        }
    }
}
