extern crate libc;

use crate::lamellae::comm::alloc::CommAllocAddr;
use crate::lamellae::FabricError;
use crate::lamellae::{
    calc_alloc_padding_size_align, decode_padding, decode_ref_count, decrement_ref_count,
    encode_ref_count_and_padding, increment_ref_count, AllocError, AllocResult, AllocationType,
    CommAlloc, CommAllocInner, CommAllocType, FabricResult, RdmaError, RdmaResult,
};
use crate::lamellar_alloc::BTreeAlloc;

use crate::lamellar_alloc::LamellarAlloc;
use std::any::type_name;
use std::any::TypeId;
use std::collections::HashSet;
use std::ffi::CString;
use std::os::raw::c_ulong;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, error, trace};

#[derive(Debug)]
pub(crate) struct RofiC {
    pub(crate) num_pes: usize,
    pub(crate) my_pe: usize,
    mem_regions: Arc<Mutex<Vec<RofiCAlloc>>>,
}

impl RofiC {
    pub(crate) fn new(provider: Option<&str>, domain: Option<&str>) -> FabricResult<Arc<Self>> {
        let prov = provider.unwrap_or("");
        let dom = domain.unwrap_or("");
        if let Err(_) = crate::lamellae::rofi_c_lamellae::rofi::rofi_c_init(prov, dom) {
            return Err(FabricError::InitError(1));
        }
        let num_pes = crate::lamellae::rofi_c_lamellae::rofi::rofi_c_get_size();
        let my_pe = crate::lamellae::rofi_c_lamellae::rofi::rofi_c_get_id();
        let world = Arc::new(RofiC {
            num_pes,
            my_pe,
            mem_regions: Arc::new(Mutex::new(Vec::new())),
        });
        Ok(world)
    }
    pub(crate) fn atomic_avail<T: 'static>(&self) -> bool {
        let t = TypeId::of::<T>();
        if t == TypeId::of::<f32>() || t == TypeId::of::<f64>() {
            return false;
        }
        crate::lamellae::rofi_c_lamellae::rofi::rofi_c_atomic_avail::<T>()
    }
    pub(crate) fn atomic_op_avail<T: 'static>(
        &self,
        op: &crate::lamellae::comm::atomic::AtomicOp<T>,
    ) -> bool {
        crate::lamellae::rofi_c_lamellae::rofi::rofi_c_atomic_op_avail(op)
    }
    pub(crate) fn alloc(
        &self,
        size: usize,
        alloc: AllocationType,
        align: usize,
    ) -> AllocResult<RofiCAlloc> {
        // compute padding and adjusted data size
        let (padding, data_size, _align) = calc_alloc_padding_size_align(size, align);

        // call into the rofi C allocator (allocate total bytes including refcount/padding)
        let base_ptr = crate::lamellae::rofi_c_lamellae::rofi::rofi_c_alloc(data_size, alloc)?;

        // construct RofiCAlloc using the constructor that expects the original user data size
        let alloc_info = RofiCAlloc::new(
            base_ptr,
            size,
            padding,
            self.my_pe,
            self.num_pes,
            AllocTable::Fabric(self.mem_regions.clone()),
        )?;

        // register in mem_regions
        let mut regions = self.mem_regions.lock().unwrap();
        regions.push(alloc_info.clone());
        Ok(alloc_info)
    }
    pub(crate) fn get_alloc_from_start_addr(&self, addr: CommAllocAddr) -> AllocResult<RofiCAlloc> {
        let regions = self.mem_regions.lock().unwrap();
        for a in regions.iter() {
            if a.start() == addr.0 {
                return Ok(a.clone());
            }
        }
        Err(AllocError::LocalNotFound(addr))
    }
    pub(crate) fn clear_allocs(&self) -> Result<(), ()> {
        let mut allocs = self.mem_regions.lock().unwrap();
        // RofiCAlloc's Drop impl will handle freeing
        allocs.clear();
        Ok(())
    }
    pub(crate) fn barrier(&self) -> Result<(), ()> {
        crate::lamellae::rofi_c_lamellae::rofi::rofi_c_barrier();
        Ok(())
    }
    pub(crate) fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> AllocResult<usize> {
        crate::lamellae::rofi_c_lamellae::rofi::rofi_c_local_addr(remote_pe, remote_addr)
    }

    pub(crate) fn one_sided_alloc_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
        num_bytes: usize,
    ) -> CommAlloc {
        if let Ok(local_addr) =
            crate::lamellae::rofi_c_lamellae::rofi::rofi_c_local_addr(remote_pe, remote_addr)
        {
            let regions = self.mem_regions.lock().unwrap();
            for a in regions.iter() {
                let start = a.start();
                if start <= local_addr && local_addr + num_bytes <= start + a.num_bytes() {
                    let offset = local_addr - start;
                    if let Ok(sub) = a.sub_alloc(offset, num_bytes) {
                        return OneSidedRofiCAlloc { alloc: sub }.into();
                    }
                }
            }
        }
        panic!(
            "unable to find allocation for remote pe: {} addr: {:x} num_bytes: {}",
            remote_pe, remote_addr, num_bytes
        );
    }

    pub(crate) fn local_alloc_and_offset_from_remote_pe_and_addr(
        &self,
        remote_pe: usize,
        remote_addr: usize,
    ) -> Option<(CommAlloc, usize)> {
        if let Ok(local_addr) =
            crate::lamellae::rofi_c_lamellae::rofi::rofi_c_local_addr(remote_pe, remote_addr)
        {
            let regions = self.mem_regions.lock().unwrap();
            for a in regions.iter() {
                let start = a.start();
                if start <= local_addr && local_addr < start + a.num_bytes() {
                    let offset = local_addr - start;
                    return Some((a.clone().into(), offset));
                }
            }
        }
        None
    }

    pub(crate) fn remote_addr(&self, pe: usize, local_addr: usize) -> AllocResult<usize> {
        crate::lamellae::rofi_c_lamellae::rofi::rofi_c_remote_addr(pe, local_addr)
    }

    pub(crate) fn wait_all(&self) -> Result<(), ()> {
        let _ = crate::lamellae::rofi_c_lamellae::rofi::rofi_c_wait();
        Ok(())
    }

    pub(crate) fn thread_wait(&self) -> Result<(), ()> {
        let _ = crate::lamellae::rofi_c_lamellae::rofi::rofi_c_wait();
        Ok(())
    }

    pub(crate) fn progress_all(&self) -> Result<(), ()> {
        let _ = crate::lamellae::rofi_c_lamellae::rofi::rofi_c_flush();
        Ok(())
    }

    pub(crate) fn thread_progress(&self) -> Result<(), ()> {
        let _ = crate::lamellae::rofi_c_lamellae::rofi::rofi_c_flush();
        Ok(())
    }
}

#[derive(Clone)]
enum AllocTable {
    Fabric(Arc<Mutex<Vec<RofiCAlloc>>>),
    Runtime(BTreeAlloc, usize, Arc<Mutex<Vec<RofiCAlloc>>>),
}

pub(crate) struct RofiCAlloc {
    pub(crate) base_data: *mut u8,
    pub(crate) base_data_num_bytes: usize,
    pub(crate) sub_data: *mut u8,
    pub(crate) sub_data_num_bytes: usize,
    pub(crate) my_pe: usize,
    pub(crate) num_pes: usize,
    fabric_ref_cnt_offset: usize,
    rt_ref_cnt_offset: usize,
    alloc_table: AllocTable,
}

impl std::fmt::Debug for RofiCAlloc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fabric_ref_count = unsafe {
            (&*(self.base_data.add(self.fabric_ref_cnt_offset) as *const AtomicUsize))
                .load(Ordering::SeqCst)
        };
        let mut temp = f.debug_struct("RofiCAlloc");
        temp.field(
            "sub_data",
            &format_args!(
                "{:p}-{:p}, {}",
                self.sub_data,
                self.sub_data.wrapping_add(self.sub_data_num_bytes),
                self.sub_data_num_bytes
            ),
        )
        .field(
            "base_data",
            &format_args!(
                "{:p}-{:p}, {}",
                self.base_data,
                self.base_data.wrapping_add(self.base_data_num_bytes),
                self.base_data_num_bytes
            ),
        )
        .field("my_pe", &self.my_pe)
        .field("num_pes", &self.num_pes)
        .field(
            "fabric_ref_cnt_offset",
            &format_args!(
                "{} ({:?}): {}",
                self.fabric_ref_cnt_offset,
                unsafe { self.base_data.add(self.fabric_ref_cnt_offset) as *const AtomicUsize },
                fabric_ref_count
            ),
        );
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            let rt_ref_count = unsafe {
                (&*(self.base_data.add(self.rt_ref_cnt_offset) as *const AtomicUsize))
                    .load(Ordering::SeqCst)
            };
            let padding = decode_padding(rt_ref_count);
            let rt_ref_count = decode_ref_count(rt_ref_count);
            temp.field(
                "rt_ref_cnt_offset",
                &format_args!(
                    "{} ({:?}): {}, {}",
                    self.rt_ref_cnt_offset,
                    unsafe { self.base_data.add(self.rt_ref_cnt_offset) as *const AtomicUsize },
                    rt_ref_count,
                    padding,
                ),
            );
        }
        temp.finish()
    }
}

impl Clone for RofiCAlloc {
    fn clone(&self) -> Self {
        trace!(
            "RofiCAlloc::clone start base={:p} sub={:p} bytes={}",
            self.base_data,
            self.sub_data,
            self.sub_data_num_bytes
        );
        let fab = self.increment_fabric_ref_count();
        trace!("RofiCAlloc::clone incremented fabric_ref_count={}", fab);
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            let rt = self.increment_rt_ref_count();
            trace!("RofiCAlloc::clone incremented rt_ref_count={}", rt);
        }
        RofiCAlloc {
            base_data: self.base_data,
            base_data_num_bytes: self.base_data_num_bytes,
            sub_data: self.sub_data,
            sub_data_num_bytes: self.sub_data_num_bytes,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset,
            alloc_table: self.alloc_table.clone(),
        }
    }
}

unsafe impl Sync for RofiCAlloc {}
unsafe impl Send for RofiCAlloc {}

impl RofiCAlloc {
    pub(crate) fn start(&self) -> usize {
        self.sub_data as usize
    }

    pub(crate) fn new(
        base_data: *mut u8,
        data_num_bytes: usize,
        padding: usize,
        my_pe: usize,
        num_pes: usize,
        alloc_table: AllocTable,
    ) -> AllocResult<RofiCAlloc> {
        // data_num_bytes here is the user-requested data size (without refcount/padding)
        let fabric_ref_cnt_offset = data_num_bytes + padding; // offset within base where refcount stored
        let rt_ref_cnt_offset = fabric_ref_cnt_offset;
        let sub_data = base_data;

        // total bytes allocated at base_data = user data + padding -- padding includes the refcount size
        let base_data_num_bytes = data_num_bytes + padding;

        let alloc = RofiCAlloc {
            base_data,
            base_data_num_bytes,
            sub_data,
            sub_data_num_bytes: base_data_num_bytes,
            my_pe,
            num_pes,
            fabric_ref_cnt_offset,
            rt_ref_cnt_offset,
            alloc_table,
        };

        // initialize ref count: 1 with padding
        let encoded = encode_ref_count_and_padding(1, padding);
        unsafe {
            (&*(alloc.base_data.add(alloc.fabric_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }

        trace!(target: "rofi", "RofiCAlloc::new base={:p} base_bytes={} sub={:p} sub_bytes={} padding={} my_pe={} num_pes={}", base_data, base_data_num_bytes, sub_data, base_data_num_bytes, padding, my_pe, num_pes);
        Ok(alloc)
    }

    pub(crate) fn num_bytes(&self) -> usize {
        self.sub_data_num_bytes
    }

    pub(crate) fn sub_alloc(&self, offset: usize, len: usize) -> AllocResult<RofiCAlloc> {
        if offset + len > self.sub_data_num_bytes {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        let new_data = unsafe { self.sub_data.add(offset) };
        trace!(
            "RofiCAlloc::sub_alloc offset={} len={} base={:p} sub={:p}",
            offset,
            len,
            self.base_data,
            self.sub_data
        );
        let fab = self.increment_fabric_ref_count();
        trace!("RofiCAlloc::sub_alloc fabric_ref_count={}", fab);
        if let AllocTable::Runtime(_, _, _) = &self.alloc_table {
            let rt = self.increment_rt_ref_count();
            trace!("RofiCAlloc::sub_alloc rt_ref_count={}", rt);
        }
        let alloc = RofiCAlloc {
            base_data: self.base_data,
            base_data_num_bytes: self.base_data_num_bytes,
            sub_data: new_data,
            sub_data_num_bytes: len,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: self.rt_ref_cnt_offset,
            alloc_table: self.alloc_table.clone(),
        };
        trace!(target: "rofi", "RofiCAlloc::sub_alloc created sub base={:p} sub={:p} bytes={}", alloc.base_data, alloc.sub_data, alloc.sub_data_num_bytes);
        Ok(alloc)
    }

    pub(crate) fn rt_alloc(
        &self,
        alloc_table: BTreeAlloc,
        offset: usize,
        padding: usize,
        len: usize,
    ) -> AllocResult<RofiCAlloc> {
        if offset + len > self.sub_data_num_bytes {
            return Err(AllocError::InvalidSubAlloc(offset, len));
        }
        // we add a new ref count at the end of this allocation for the runtime allocation
        let new_data_bytes = len - padding - std::mem::size_of::<AtomicUsize>();
        let new_data = unsafe { self.sub_data.add(offset) };

        trace!(
            "RofiCAlloc::rt_alloc offset={} len={} padding={} base={:p}",
            offset,
            len,
            padding,
            self.base_data
        );
        let fab = self.increment_fabric_ref_count();
        trace!("RofiCAlloc::rt_alloc incremented fabric_ref_count={}", fab);
        let ref_cnt_offset = offset + new_data_bytes + padding;
        let encoded = encode_ref_count_and_padding(1, padding);

        let allocs = match &self.alloc_table {
            AllocTable::Fabric(at) => at.clone(),
            AllocTable::Runtime(_, _, at) => at.clone(),
        };

        let alloc = RofiCAlloc {
            base_data: self.base_data,
            base_data_num_bytes: self.base_data_num_bytes,
            sub_data: new_data,
            sub_data_num_bytes: new_data_bytes,
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset, //keep the same ref count offset as the parent allocation if this is actually a rt alloc, it will be updated when converted to a rt_alloc
            alloc_table: AllocTable::Runtime(alloc_table, new_data as usize, allocs),
        };
        unsafe {
            (&*(alloc.base_data.add(alloc.rt_ref_cnt_offset) as *mut AtomicUsize))
                .store(encoded, Ordering::SeqCst);
        }
        trace!(target: "rofi", "RofiCAlloc::rt_alloc created rt alloc base={:p} sub={:p} sub_bytes={} rt_ref_offset={}", alloc.base_data, alloc.sub_data, alloc.sub_data_num_bytes, alloc.rt_ref_cnt_offset);
        Ok(alloc)
    }

    pub(crate) fn as_rt_alloc(self, alloc_table: BTreeAlloc) -> AllocResult<Self> {
        let allocs = match &self.alloc_table {
            AllocTable::Fabric(allocs) => allocs.clone(),
            AllocTable::Runtime(_, _, allocs) => allocs.clone(),
        };
        let ref_cnt_offset = ((self.start() - self.base_data as usize) + self.num_bytes())
            - std::mem::size_of::<AtomicUsize>();
        let encoded_ref_count = unsafe {
            (&*(self.base_data.add(ref_cnt_offset) as *const AtomicUsize)).load(Ordering::SeqCst)
        };
        let padding = decode_padding(encoded_ref_count);

        let alloc = RofiCAlloc {
            base_data: self.base_data,
            base_data_num_bytes: self.base_data_num_bytes,
            sub_data: self.sub_data,
            sub_data_num_bytes: self.sub_data_num_bytes
                - padding
                - std::mem::size_of::<AtomicUsize>(),
            my_pe: self.my_pe,
            num_pes: self.num_pes,
            fabric_ref_cnt_offset: self.fabric_ref_cnt_offset,
            rt_ref_cnt_offset: ref_cnt_offset,
            alloc_table: AllocTable::Runtime(alloc_table, self.sub_data as usize, allocs),
        };

        trace!(target: "rofi", "RofiCAlloc::as_rt_alloc base={:p} sub={:p} new_sub_bytes={} rt_ref_offset={}", alloc.base_data, alloc.sub_data, alloc.sub_data_num_bytes, alloc.rt_ref_cnt_offset);
        Ok(alloc)
    }

    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        match self.alloc_table {
            AllocTable::Fabric(_) => None,
            AllocTable::Runtime(_, _, _) => {
                let fab = self.increment_fabric_ref_count();
                let rt = self.increment_rt_ref_count();
                trace!(target: "rofi", "RofiCAlloc::leak fabric_ref_count={} rt_ref_count={} addr={:x}", fab, rt, self.start());
                Some(CommAllocAddr(self.start()))
            }
        }
    }

    pub(crate) fn increment_fabric_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.base_data.add(self.fabric_ref_cnt_offset) as *const AtomicUsize) };
        increment_ref_count(ref_count)
    }

    pub(crate) fn decrement_fabric_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.base_data.add(self.fabric_ref_cnt_offset) as *const AtomicUsize) };
        decrement_ref_count(ref_count)
    }

    pub(crate) fn increment_rt_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.base_data.add(self.rt_ref_cnt_offset) as *const AtomicUsize) };
        increment_ref_count(ref_count)
    }

    pub(crate) fn decrement_rt_ref_count(&self) -> usize {
        let ref_count =
            unsafe { &*(self.base_data.add(self.rt_ref_cnt_offset) as *const AtomicUsize) };
        decrement_ref_count(ref_count)
    }

    pub(crate) fn wait(&self) -> RdmaResult {
        let ret = crate::lamellae::rofi_c_lamellae::rofi::rofi_c_wait();
        if ret == 0 {
            Ok(())
        } else {
            Err(RdmaError::FabricWaitError(ret))
        }
    }

    pub(crate) unsafe fn zeroize_bytes(&self) {
        let u8_slice = std::slice::from_raw_parts_mut(self.sub_data, self.sub_data_num_bytes);
        u8_slice.fill(0);
    }
}

impl Drop for RofiCAlloc {
    fn drop(&mut self) {
        trace!(
            "RofiCAlloc::drop enter base={:p} sub={:p} bytes={}",
            self.base_data,
            self.sub_data,
            self.sub_data_num_bytes
        );
        let fabric_ref_count = self.decrement_fabric_ref_count();
        trace!(
            "RofiCAlloc::drop after decrement fabric_ref_count={}",
            fabric_ref_count
        );
        match &self.alloc_table {
            AllocTable::Fabric(allocs) => {
                if fabric_ref_count == 2 {
                    trace!(
                        "RofiCAlloc::drop freeing fabric alloc base={:p}",
                        self.base_data
                    );
                    let mut allocs = allocs.lock().unwrap();
                    let len = allocs.len();
                    allocs.retain(|a| a.base_data != self.base_data);
                    if len == allocs.len() {
                        error!("RofiCAlloc::drop failed to free alloc: {:?}", self);
                        panic!("failed to free alloc: {:?}", self);
                    }
                    unsafe {
                        crate::lamellae::rofi_c_lamellae::rofi::rofi_c_release(
                            self.base_data as usize,
                        )
                    };
                }
            }
            AllocTable::Runtime(rt_alloc_table, addr, allocs) => {
                let rt_ref_count = self.decrement_rt_ref_count();
                trace!(
                    "RofiCAlloc::drop after decrement rt_ref_count={}",
                    rt_ref_count
                );
                if rt_ref_count == 1 {
                    trace!("RofiCAlloc::drop freeing runtime alloc addr={:x}", addr);
                    rt_alloc_table.free(*addr).expect(&format!(
                        "[{:?}] Error removing from runtime alloc table {:x}",
                        std::thread::current().id(),
                        addr
                    ));
                }
                if fabric_ref_count == 2 {
                    trace!(
                        "RofiCAlloc::drop freeing fabric alloc (runtime) base={:p}",
                        self.base_data
                    );
                    let mut allocs = allocs.lock().unwrap();
                    let len = allocs.len();
                    allocs.retain(|a| a.base_data != self.base_data);
                    if len == allocs.len() {
                        error!("RofiCAlloc::drop failed to free alloc: {:?}", self);
                        panic!("failed to free alloc: {:?}", self);
                    }
                }
            }
        }
    }
}

impl From<RofiCAlloc> for CommAlloc {
    fn from(alloc: RofiCAlloc) -> Self {
        CommAlloc {
            inner_alloc: CommAllocInner::RofiCAlloc(alloc),
            alloc_type: CommAllocType::Fabric,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OneSidedRofiCAlloc {
    pub(crate) alloc: RofiCAlloc,
}

impl OneSidedRofiCAlloc {
    pub(crate) fn start(&self) -> usize {
        self.alloc.start()
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.alloc.num_bytes()
    }
    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> Option<OneSidedRofiCAlloc> {
        self.alloc
            .sub_alloc(offset, size)
            .ok()
            .map(|a| OneSidedRofiCAlloc { alloc: a })
    }
    pub(crate) fn wait(&self) {
        self.alloc
            .wait()
            .expect("error waiting on onesided rofi-c alloc");
    }
}

impl From<OneSidedRofiCAlloc> for CommAlloc {
    fn from(alloc: OneSidedRofiCAlloc) -> Self {
        CommAlloc {
            inner_alloc: CommAllocInner::OneSidedRofiCAlloc(alloc),
            alloc_type: CommAllocType::Remote,
        }
    }
}
