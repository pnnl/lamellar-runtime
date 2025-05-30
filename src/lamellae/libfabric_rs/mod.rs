use libfabric::RemoteMemoryAddress;
use libfabric::mr;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub(crate) mod libfabric_sync;
pub(crate) mod libfabric_async;


unsafe impl Send for AllocInfoManager {}
unsafe impl Sync for AllocInfoManager {}

pub(crate) struct AllocInfoManager {
    pub(crate) mr_info_table: RwLock<Vec<AllocInfo>>,
    mr_next_key: AtomicUsize,
    page_size: usize,
}

impl AllocInfoManager {
    pub(crate) fn new() -> Self {
        Self {
            mr_info_table: RwLock::new(Vec::new()),
            mr_next_key: AtomicUsize::new(0),
            page_size: page_size::get(),
        }
    }

    pub(crate) fn insert(&self, alloc: AllocInfo) {
        self.mr_info_table.write().push(alloc)
    }

    pub(crate) fn remove(&self, mem_addr: &usize) {
        let mut table = self.mr_info_table.write();
        let idx = table
            .iter()
            .position(|e| e.start() == *mem_addr)
            .expect("Error! Invalid memory address");

        table.remove(idx);
    }

    pub(crate) fn local_addr(&self, remote_pe: &usize, remote_addr: &RemoteMemoryAddress) -> Option<usize> {
        let table = self.mr_info_table.read();
        if let Some(alloc_info) = table
            .iter()
            .find(|x| x.remote_contains_remote(remote_pe, remote_addr))
        {
            Some(unsafe{remote_addr.offset_from(alloc_info.remote_start(&remote_pe))} as usize + alloc_info.start())
        } else {
            None
        }
    }

    pub(crate) fn remote_addr(&self, remote_pe: &usize, local_addr: &usize) -> Option<RemoteMemoryAddress> {
        let table = self.mr_info_table.read();
        if let Some(alloc_info) = table.iter().find(|x| x.contains(local_addr)) {
            if let Some(remote_alloc_info) = alloc_info.remote_allocs.get(remote_pe) {
                Some(unsafe{remote_alloc_info.start().add(local_addr - alloc_info.start())})
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    pub(crate) fn next_key(&self) -> usize {
        self.mr_next_key.fetch_add(1, Ordering::SeqCst)
    }
}
#[derive(Clone)]
pub(crate) struct RemoteAllocInfo {
    remote_mem_info: libfabric::RemoteMemAddressInfo,
    start_addr: RemoteMemoryAddress,
    end_addr: RemoteMemoryAddress,
}

impl RemoteAllocInfo {
    pub(crate) fn new (
        remote_mem_info: libfabric::RemoteMemAddressInfo,
    ) -> Self {
        Self {
            start_addr: remote_mem_info.mem_address(),
            end_addr: unsafe{remote_mem_info.mem_address().add(remote_mem_info.mem_len())},
            remote_mem_info,
        }
    }

    pub(crate) fn start(&self) -> &RemoteMemoryAddress {
        &self.start_addr
    }

    pub(crate) fn end(&self) -> &RemoteMemoryAddress {
        &self.end_addr
    }

    pub(crate) fn key(&self) -> libfabric::mr::MappedMemoryRegionKey {
        self.remote_mem_info.key()
    }

    pub(crate) fn contains_remote(&self, addr: &RemoteMemoryAddress) -> bool {
        &self.start_addr <= addr
            && addr < &self.end_addr
    }

    pub(crate) fn contains_local(&self, addr: &usize) -> bool {
        &(self.start_addr.as_ptr() as usize) <= addr
            && addr < &(self.end_addr.as_ptr() as usize)
    }
}

pub(crate) struct AllocInfo {
    _mem: memmap::MmapMut,
    mr: Arc<libfabric::mr::MemoryRegion>,
    range: std::ops::Range<usize>,
    remote_allocs: HashMap<usize, RemoteAllocInfo>,
}

impl AllocInfo {
    pub(crate) fn new(
        mem: memmap::MmapMut,
        mr: Arc<libfabric::mr::MemoryRegion>,
        remote_allocs: HashMap<usize, RemoteAllocInfo>,
    ) -> Result<Self, libfabric::error::Error> {
        let start = mem.as_ptr() as usize;
        let end = start + mem.len();

        Ok(Self {
            _mem: mem,
            mr,
            range: std::ops::Range { start, end },
            remote_allocs,
        })
    }

    pub(crate) fn start(&self) -> usize {
        self.range.start
    }

    #[allow(dead_code)]
    pub(crate) fn remote_start(&self, remote_id: &usize) -> &RemoteMemoryAddress {
        self.remote_allocs
            .get(remote_id)
            .expect(&format!(
                "PE {} is not part of the sub allocation group",
                remote_id
            ))
            .start()
    }

    #[allow(dead_code)]
    pub(crate) fn remote_end(&self, remote_id: &usize) -> &RemoteMemoryAddress {
        self.remote_allocs
            .get(remote_id)
            .expect(&format!(
                "PE {} is not part of the sub allocation group",
                remote_id
            ))
            .end()
    }

    #[allow(dead_code)]
    pub(crate) fn end(&self) -> usize {
        self.range.end
    }

    #[allow(dead_code)]
    pub(crate) fn remote_key(&self, remote_id: &usize) -> libfabric::mr::MappedMemoryRegionKey {
        self.remote_allocs
            .get(remote_id)
            .expect(&format!(
                "PE {} is not part of the sub allocation group",
                remote_id
            ))
            .key()
    }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        self.range.contains(addr)
    }

    #[allow(dead_code)]
    pub(crate) fn remote_contains_local(&self, remote_id: &usize, addr: &usize) -> bool {
        self.remote_allocs
            .get(remote_id)
            .expect(&format!(
                "PE {} is not part of the sub allocation group",
                remote_id
            ))
            .contains_local(addr)
    }

    #[allow(dead_code)]
    pub(crate) fn remote_contains_remote(&self, remote_id: &usize, addr: &RemoteMemoryAddress) -> bool {
        self.remote_allocs
            .get(remote_id)
            .expect(&format!(
                "PE {} is not part of the sub allocation group",
                remote_id
            ))
            .contains_remote(addr)
    }

    pub(crate) fn remote_info(&self, remote_pe: &usize) -> Option<RemoteAllocInfo> {
        self.remote_allocs.get(remote_pe).cloned()
    }

    pub(crate) fn mr(&self) -> Arc<libfabric::mr::MemoryRegion> {
        self.mr.clone()
    }

}

fn euclid_rem(a: i64, b: i64) -> usize {
    let r = a % b;

    if r >= 0 {
        r as usize
    } else {
        (r + b.abs()) as usize
    }
}
