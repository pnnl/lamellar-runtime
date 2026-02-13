#[cfg(feature = "enable-libfabric-async")]
use crate::lamellae::libfabric_async_lamellae::fabric::{
    LibfabricAsyncAlloc, OneSidedLibfabricAsyncAlloc,
};
use crate::lamellae::collective::{CollectiveAllBroadcastIntoBufferOpHandle, CollectiveAllBroadcastOpHandle, CollectiveAllGatherIntoBufferOpHandle, CollectiveAllGatherOpHandle, CollectiveAllReduceInPlaceOpHandle, CollectiveAllReduceIntoBufferOpHandle, CollectiveAllReduceOpHandle, CollectiveBroadcastIntoBufferOpHandle, CollectiveBroadcastOpHandle, CollectiveGatherIntoBufferOpHandle, CollectiveGatherOpHandle, CollectiveReduceInPlaceOpHandle, CollectiveReduceIntoBufferOpHandle, CollectiveReduceOpHandle, CommAllocCollectiveAllBroadcast, CommAllocCollectiveAllGather, CommAllocCollectiveAllReduce, CommAllocCollectiveBroadcast, CommAllocCollectiveGather, CommAllocCollectiveReduce, ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer};
#[cfg(feature = "enable-libfabric")]
use crate::lamellae::libfabric_lamellae::fabric::{LibfabricAlloc, OneSidedLibfabricAlloc};
#[cfg(feature = "enable-libfabric-mt")]
use crate::lamellae::libfabric_lamellae_mt::fabric::{LibfabricMtAlloc, OneSidedLibfabricMtAlloc};
#[cfg(feature = "enable-rofi-c")]
use crate::lamellae::rofi_c_lamellae::fabric::{OneSidedRofiCAlloc, RofiCAlloc};
#[cfg(feature = "enable-ucx")]
use crate::lamellae::ucx_lamellae::fabric::{OneSidedUcxAlloc, UcxAlloc};
#[cfg(feature = "enable-ucx-mt")]
use crate::lamellae::ucx_lamellae_mt::fabric::{OneSidedUcxMtAlloc, UcxMtAlloc};

use crate::{
    active_messaging::AMCounters,
    lamellae::{
        local_lamellae::comm::LocalAlloc,
        shmem_lamellae::fabric::{OneSidedShmemAlloc, ShmemAlloc},
        AtomicOp, CommAllocAtomic, CommAllocRdma, CommSlice, RdmaGetBufferHandle, RdmaGetHandle,
        RdmaGetIntoBufferHandle,
    },
    memregion::{AsLamellarBuffer, LamellarBuffer, MemregionRdmaInputInner},
    scheduler::Scheduler,
    AtomicCompareExchangeOpHandle, AtomicFetchOpHandle, AtomicOpHandle, Deserialize, RdmaHandle,
    Remote, Serialize,
};

use derive_more::{Add, Into, Sub};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tracing::trace;

// Masks and functions for encoding/decoding reference count and padding into a single usize
// to be used when tracking allocations from the fabric and runtime sides.
pub(crate) const PADDING_MASK: usize = 0xFF << (usize::BITS - 8);
pub(crate) const COUNT_MASK: usize = !PADDING_MASK;

pub(crate) fn encode_ref_count_and_padding(ref_count: usize, padding: usize) -> usize {
    assert!(padding < 256, "Padding must fit within 8 bits");
    (ref_count & COUNT_MASK) | ((padding << (usize::BITS - 8)) & PADDING_MASK)
}


#[cfg(any(feature = "enable-libfabric", feature = "enable-libfabric-mt", feature = "enable-libfabric-async"))]
pub(crate) fn decode_ref_count_and_padding(encoded: usize) -> (usize, usize) {
    let ref_count = encoded & COUNT_MASK;
    let padding = (encoded & PADDING_MASK) >> (usize::BITS - 8);
    (ref_count, padding)
}

pub(crate) fn decode_padding(encoded: usize) -> usize {
    (encoded & PADDING_MASK) >> (usize::BITS - 8)
}

pub(crate) fn decode_ref_count(encoded: usize) -> usize {
    encoded & COUNT_MASK
}

// WARNING: in theory this could overflow if ref count exceeds 56^2 -1 = 72057594037927935
// which if we had that many references to a single allocation something has likely has gone very wrong
pub(crate) fn increment_ref_count(counter: &AtomicUsize) -> usize {
    decode_ref_count(counter.fetch_add(1, Ordering::SeqCst))
}

pub(crate) fn decrement_ref_count(counter: &AtomicUsize) -> usize {
    decode_ref_count(counter.fetch_sub(1, Ordering::SeqCst))
}

// pub(crate) fn get_padding(counter: &AtomicUsize) -> usize {
//     decode_padding(counter.load(Ordering::SeqCst))
// }

#[cfg(any(feature = "enable-libfabric", feature = "enable-libfabric-mt", feature = "enable-libfabric-async"))]
pub(crate) fn get_ref_count(counter: &AtomicUsize) -> usize {
    decode_ref_count(counter.load(Ordering::SeqCst))
}
// --------------------------------------------------------------------

pub(crate) fn calc_alloc_padding_size_align(size: usize, align: usize) -> (usize, usize, usize) {
    // add space for ref count
    let ref_cnt_size = std::mem::size_of::<AtomicUsize>();
    let ref_cnt_align = std::mem::align_of::<AtomicUsize>();
    let padding = (ref_cnt_align - (size % ref_cnt_align)) % ref_cnt_align;
    let size = size + ref_cnt_size + padding;
    let align = std::cmp::max(align, ref_cnt_align);
    (padding, size, align)
}
//

#[derive(Clone, Debug)]
pub(crate) enum CommAllocInner {
    Raw(usize, usize), //address, size
    LocalAlloc(Arc<LocalAlloc>),
    ShmemAlloc(ShmemAlloc),
    OneSidedShmemAlloc(OneSidedShmemAlloc),
    #[cfg(feature = "enable-libfabric")]
    LibfabricAlloc(LibfabricAlloc),
    #[cfg(feature = "enable-libfabric-mt")]
    LibfabricMtAlloc(LibfabricMtAlloc),
    #[cfg(feature = "enable-libfabric-async")]
    LibfabricAsyncAlloc(LibfabricAsyncAlloc),
    #[cfg(feature = "enable-libfabric")]
    OneSidedLibfabricAlloc(OneSidedLibfabricAlloc),
    #[cfg(feature = "enable-libfabric-mt")]
    OneSidedLibfabricMtAlloc(OneSidedLibfabricMtAlloc),
    #[cfg(feature = "enable-libfabric-async")]
    OneSidedLibfabricAsyncAlloc(OneSidedLibfabricAsyncAlloc),
    #[cfg(feature = "enable-rofi-c")]
    RofiCAlloc(RofiCAlloc),
    #[cfg(feature = "enable-rofi-c")]
    OneSidedRofiCAlloc(OneSidedRofiCAlloc),
    #[cfg(feature = "enable-ucx")]
    UcxAlloc(UcxAlloc),
    #[cfg(feature = "enable-ucx-mt")]
    UcxMtAlloc(UcxMtAlloc),
    #[cfg(feature = "enable-ucx")]
    OneSidedUcxAlloc(OneSidedUcxAlloc),
    #[cfg(feature = "enable-ucx-mt")]
    OneSidedUcxMtAlloc(OneSidedUcxMtAlloc),
}

impl CommAllocInner {
    pub(crate) fn addr(&self) -> CommAllocAddr {
        match self {
            CommAllocInner::Raw(addr, _) => CommAllocAddr(*addr),
            CommAllocInner::LocalAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            CommAllocInner::ShmemAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocAddr(inner_alloc.start())
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocAddr(inner_alloc.start())
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocAddr(inner_alloc.start())
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => CommAllocAddr(inner_alloc.start()),
        }
    }

    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        match self {
            CommAllocInner::Raw(_, _) => None,
            CommAllocInner::LocalAlloc(inner_alloc) => inner_alloc.leak(),
            CommAllocInner::ShmemAlloc(inner_alloc) => inner_alloc.leak(),
            CommAllocInner::OneSidedShmemAlloc(_inner_alloc) => {
                panic!("OneSidedShmemAlloc cannot be leaked")
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => inner_alloc.leak(),
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(_inner_alloc) => {
                panic!("OneSidedLibfabricAlloc cannot be leaked")
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => inner_alloc.leak(),
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(_inner_alloc) => {
                panic!("OneSidedLibfabricMtAlloc cannot be leaked")
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => inner_alloc.leak(),
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(_inner_alloc) => {
                panic!("OneSidedLibfabricAsyncAlloc cannot be leaked")
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => inner_alloc.leak(),
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(_inner_alloc) => {
                panic!("OneSidedRofiCAlloc cannot be leaked")
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => inner_alloc.leak(),
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => inner_alloc.leak(),
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(_inner_alloc) => {
                panic!("OneSidedUcxAlloc cannot be leaked")
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(_inner_alloc) => {
                panic!("OneSidedUcxMtAlloc cannot be leaked")
            }
        }
    }
    pub(crate) fn size(&self) -> usize {
        match self {
            CommAllocInner::Raw(_, size) => *size,
            CommAllocInner::LocalAlloc(inner_alloc) => inner_alloc.num_bytes(),
            CommAllocInner::ShmemAlloc(inner_alloc) => inner_alloc.num_bytes(),
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => inner_alloc.num_bytes(),
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => inner_alloc.num_bytes(),
        }
    }
    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> CommAllocInner {
        trace!("sub_alloc offset: {} size: {}", offset, size);
        assert!(offset + size <= self.size());
        match self {
            CommAllocInner::Raw(addr, _) => CommAllocInner::Raw(*addr + offset, size),
            CommAllocInner::LocalAlloc(inner_alloc) => CommAllocInner::LocalAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            CommAllocInner::ShmemAlloc(inner_alloc) => CommAllocInner::ShmemAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => CommAllocInner::OneSidedShmemAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => CommAllocInner::LibfabricAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocInner::OneSidedLibfabricAlloc(
                    inner_alloc
                        .sub_alloc(offset, size)
                        .expect("Invalid sub allocation"),
                )
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => CommAllocInner::LibfabricMtAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocInner::OneSidedLibfabricMtAlloc(
                    inner_alloc
                        .sub_alloc(offset, size)
                        .expect("Invalid sub allocation"),
                )
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocInner::LibfabricAsyncAlloc(
                    inner_alloc
                        .sub_alloc(offset, size)
                        .expect("Invalid sub allocation"),
                )
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocInner::OneSidedLibfabricAsyncAlloc(
                    inner_alloc
                        .sub_alloc(offset, size)
                        .expect("Invalid sub allocation"),
                )
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => CommAllocInner::RofiCAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => CommAllocInner::OneSidedRofiCAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => CommAllocInner::UcxAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => CommAllocInner::UcxMtAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => CommAllocInner::OneSidedUcxAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => CommAllocInner::OneSidedUcxMtAlloc(
                inner_alloc
                    .sub_alloc(offset, size)
                    .expect("Invalid sub allocation"),
            ),
        }
    }
    pub(crate) fn contains(&self, addr: &usize) -> bool {
        let my_addr: usize = self.addr().into();
        my_addr <= *addr && *addr < my_addr + self.size()
    }

    pub(crate) fn wait(&self) {
        match self {
            CommAllocInner::Raw(_, _) => {}
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.wait();
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.wait();
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                inner_alloc.wait();
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc
                    .wait()
                    .expect("error waiting on libfabric alloc");
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                inner_alloc
                    .alloc
                    .wait()
                    .expect("error waiting on onesided libfabric alloc");
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                inner_alloc
                    .wait()
                    .expect("error waiting on libfabric mt alloc");
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                inner_alloc
                    .alloc
                    .wait()
                    .expect("error waiting on onesided libfabric mt alloc");
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc
                    .wait()
                    .expect("error waiting on libfabric async alloc");
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc
                    .alloc
                    .wait()
                    .expect("error waiting on onesided libfabric async alloc");
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                inner_alloc.wait();
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.wait();
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                inner_alloc.alloc.wait();
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                inner_alloc.alloc.wait();
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                inner_alloc.wait().expect("error waiting on rofi-c alloc");
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                inner_alloc
                    .alloc
                    .wait()
                    .expect("error waiting on onesided rofi-c alloc");
            }
        }
    }
}

impl CommAllocRdma for CommAllocInner {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put(inner_alloc, scheduler, counters, src, pe, offset)
            }
        }
    }
    fn put_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        src: T,
        pe: usize,
        offset: usize,
    ) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_blocking(inner_alloc, scheduler, src, pe, offset)
            }
        }
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        trace!("CommAllocInner::put_unmanaged called");
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_unmanaged(inner_alloc, src, pe, offset)
            }
        }
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer(inner_alloc, scheduler, counters, src, pe, offset)
            }
        }
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_buffer_unmanaged(inner_alloc, src, pe, offset)
            }
        }
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all(inner_alloc, scheduler, counters, src, offset)
            }
        }
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_unmanaged(inner_alloc, src, offset)
            }
        }
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer(inner_alloc, scheduler, counters, src, offset)
            }
        }
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::put_all_buffer_unmanaged(inner_alloc, src, offset)
            }
        }
    }

    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::get(inner_alloc, scheduler, counters, pe, offset)
            }
        }
    }

    fn blocking_get<T: Remote>(&self, scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get(inner_alloc, scheduler, pe, offset)
            }
        }
    }

    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::get_buffer(inner_alloc, scheduler, counters, pe, offset, len)
            }
        }
    }
    fn blocking_get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_buffer(inner_alloc, scheduler, pe, offset, len)
            }
        }
    }
    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer(inner_alloc, scheduler, counters, pe, offset, dst)
            }
        }
    }
    fn blocking_get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::blocking_get_into_buffer(inner_alloc, scheduler, pe, offset, dst)
            }
        }
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                CommAllocRdma::get_into_buffer_unmanaged(inner_alloc, pe, offset, dst)
            }
        }
    }
}

impl CommAllocAtomic for CommAllocInner {
    fn atomic_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_op(scheduler, counters, op, pe, offset)
            }
        }
    }
    fn atomic_op_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_op_blocking(scheduler, op, pe, offset)
            }
        }
    }
    fn atomic_op_unmanaged<T: Remote>(&self, op: AtomicOp<T>, pe: usize, offset: usize) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_op_unmanaged(op, pe, offset)
            }
        }
    }
    fn atomic_op_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        offset: usize,
    ) -> AtomicOpHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all(scheduler, counters, op, offset)
            }
        }
    }
    fn atomic_op_all_unmanaged<T: Remote>(&self, op: AtomicOp<T>, offset: usize) {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_op_all_unmanaged(op, offset)
            }
        }
    }
    fn atomic_fetch_op<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> AtomicFetchOpHandle<T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            }
        }
    }
    fn atomic_fetch_op_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        op: AtomicOp<T>,
        pe: usize,
        offset: usize,
    ) -> T {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_fetch_op_blocking(scheduler, op, pe, offset)
            }
        }
    }
    fn atomic_compare_exchange<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> AtomicCompareExchangeOpHandle<T> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.reduce_all(scheduler, counters, op)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange(scheduler, counters, current, new, pe, offset)
            }
        }
    }
    fn atomic_compare_exchange_blocking<T: Remote + PartialEq>(
        &self,
        scheduler: &Arc<Scheduler>,
        current: T,
        new: T,
        pe: usize,
        offset: usize,
    ) -> Result<T, T> {
        match self {
            CommAllocInner::Raw(_addr, _size) => {
                panic!("Raw allocation not supported")
            }
            CommAllocInner::LocalAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            CommAllocInner::ShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-mt")]
            CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-libfabric-async")]
            CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::UcxAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::UcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-ucx")]
            CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-ucx-mt")]
            CommAllocInner::OneSidedUcxMtAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::RofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
            #[cfg(feature = "enable-rofi-c")]
            CommAllocInner::OneSidedRofiCAlloc(inner_alloc) => {
                inner_alloc.atomic_compare_exchange_blocking(scheduler, current, new, pe, offset)
            }
        }
    }
}

impl CommAllocCollectiveAllReduce for CommAllocInner {
    fn reduce_all<T: Remote> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
    ) -> CollectiveAllReduceOpHandle<T> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.reduce_all(scheduler, counters, op)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
    fn reduce_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllReduceIntoBufferOpHandle<T, B> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.reduce_all_into_buffer(scheduler, counters, op, buffer)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
    fn reduce_all_in_place<T: Remote> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
    ) -> CollectiveAllReduceInPlaceOpHandle<T> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.reduce_all_in_place(scheduler, counters, op)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
}

impl CommAllocCollectiveReduce for CommAllocInner {
    fn reduce<T: Remote> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        root_pe: usize,
    ) -> CollectiveReduceOpHandle<T> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {

                inner_alloc.reduce(scheduler, counters, op, root_pe)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
    fn reduce_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveReduceIntoBufferOpHandle<T, B> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.reduce_into_buffer(scheduler, counters, op, root_or_buffer)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
    fn reduce_in_place<T: Remote> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        op: ReduceOp,
        root_pe: usize,
    ) -> CollectiveReduceInPlaceOpHandle<T> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.reduce_in_place(scheduler, counters, op, root_pe)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
}


impl CommAllocCollectiveAllGather for CommAllocInner {
    fn gather_all<T: Remote> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
    ) -> CollectiveAllGatherOpHandle<T> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.gather_all(scheduler, counters)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
    fn gather_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllGatherIntoBufferOpHandle<T, B> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.gather_all_into_buffer(scheduler, counters, buffer)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
}

impl CommAllocCollectiveGather for CommAllocInner {
    fn gather<T: Remote> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        root_pe: usize,
    ) -> CollectiveGatherOpHandle<T> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {

                inner_alloc.gather(scheduler, counters, root_pe)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
    fn gather_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        root_or_buffer: RootOrLamellarBuffer<T, B>
    ) -> CollectiveGatherIntoBufferOpHandle<T, B> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.gather_into_buffer(scheduler, counters, root_or_buffer)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
}


impl CommAllocCollectiveAllBroadcast for CommAllocInner {
    fn broadcast_all<T: Remote> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
    ) -> CollectiveAllBroadcastOpHandle<T> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.broadcast_all(scheduler, counters)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
    fn broadcast_all_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        buffer: LamellarBuffer<T, B>,
    ) -> CollectiveAllBroadcastIntoBufferOpHandle<T, B> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.broadcast_all_into_buffer(scheduler, counters, buffer)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
}



impl CommAllocCollectiveBroadcast for CommAllocInner {
    fn broadcast<T: Remote> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        root_pe: usize,
    ) -> CollectiveBroadcastOpHandle<T> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {

                inner_alloc.broadcast(scheduler, counters, root_pe)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
    fn broadcast_into_buffer<T: Remote, B: AsLamellarBuffer<T>> (
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Vec<Arc<AMCounters>>,
        root_or_buffer: RootSrcOrLamellarBuffer<T, B>
    ) -> CollectiveBroadcastIntoBufferOpHandle<T, B> {
        match self {
            // CommAllocInner::Raw(_addr, _size) => {
            //     panic!("Raw allocation not supported")
            // }
            // CommAllocInner::LocalAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::ShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // CommAllocInner::OneSidedShmemAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            #[cfg(feature = "enable-libfabric")]
            CommAllocInner::LibfabricAlloc(inner_alloc) => {
                inner_alloc.broadcast_into_buffer(scheduler, counters, root_or_buffer)
            }
            _ => {
                panic!("Collective reduce not supported for this CommAlloc type")
            }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::LibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricMtAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric")]
            // CommAllocInner::OneSidedLibfabricAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::LibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-libfabric-async")]
            // CommAllocInner::OneSidedLibfabricAsyncAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::UcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
            // #[cfg(feature = "enable-ucx")]
            // CommAllocInner::OneSidedUcxAlloc(inner_alloc) => {
            //     inner_alloc.atomic_fetch_op(scheduler, counters, op, pe, offset)
            // }
        }
    }
}


#[derive(Clone, Debug)]
pub(crate) struct CommAlloc {
    pub(crate) inner_alloc: Arc<CommAllocInner>,
    // pub(crate) alloc_type: CommAllocType,
}

impl CommAlloc {
    //#[tracing::instrument(skip(self), level = "debug")]
    pub(crate) fn as_comm_slice<T>(&self) -> CommSlice<T> {
        CommSlice {
            inner_alloc: Arc::clone(&self.inner_alloc),
            _phantom: std::marker::PhantomData,
        }
    }
    //#[tracing::instrument(level = "debug")]
    pub(crate) fn comm_slice_at_byte_offset<T>(
        &self,
        offset: usize,
        num_elems: usize,
    ) -> CommSlice<T> {
        trace!(
            "{:?} offset: {}  num_elems: {} bytes: {}",
            self,
            offset,
            num_elems,
            num_elems * std::mem::size_of::<T>()
        );
        assert!(offset + num_elems * std::mem::size_of::<T>() <= self.num_bytes());
        CommSlice {
            inner_alloc: Arc::new(
                self.inner_alloc
                    .sub_alloc(offset, num_elems * std::mem::size_of::<T>()),
            ),
            _phantom: std::marker::PhantomData,
        }
    }

    pub(crate) unsafe fn as_ptr<T>(&self) -> *const T {
        self.inner_alloc.addr().as_ptr::<T>()
    }
    pub(crate) unsafe fn as_mut_ptr<T>(&self) -> *mut T {
        self.inner_alloc.addr().as_mut_ptr::<T>()
    }
    // pub(crate) unsafe fn as_ref<T>(&self) -> Option<&T> {
    //     self.as_ptr::<T>().as_ref()
    // }
    pub(crate) fn comm_addr(&self) -> CommAllocAddr {
        self.inner_alloc.addr()
    }
    pub(crate) fn num_bytes(&self) -> usize {
        self.inner_alloc.size()
    }

    // For Allocs that have been created using rt_alloc, this functions
    // will consume the CommAlloc and return the underlying address
    // and prevent the memory from being freed when the CommAlloc is dropped (if it was the last reference)
    // if the CommAlloc was allocated directly from the fabric then None is returned
    // since we cannot guarantee the memory will remain valid
    pub(crate) fn leak(self) -> Option<CommAllocAddr> {
        (*self.inner_alloc).clone().leak()
    }
    pub(crate) fn sub_alloc(&self, offset: usize, size: usize) -> CommAlloc {
        CommAlloc {
            inner_alloc: Arc::new(self.inner_alloc.sub_alloc(offset, size)),
            // alloc_type: self.alloc_type,
        }
    }
}

impl CommAllocRdma for CommAlloc {
    fn put<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.inner_alloc.put(scheduler, counters, src, pe, offset)
    }
    fn put_blocking<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        src: T,
        pe: usize,
        offset: usize,
    ) {
        self.inner_alloc.put_blocking(scheduler, src, pe, offset)
    }
    fn put_unmanaged<T: Remote>(&self, src: T, pe: usize, offset: usize) {
        self.inner_alloc.put_unmanaged(src, pe, offset)
    }
    fn put_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.inner_alloc
            .put_buffer(scheduler, counters, src, pe, offset)
    }
    fn put_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        pe: usize,
        offset: usize,
    ) {
        self.inner_alloc.put_buffer_unmanaged(src, pe, offset)
    }
    fn put_all<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: T,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.inner_alloc.put_all(scheduler, counters, src, offset)
    }
    fn put_all_unmanaged<T: Remote>(&self, src: T, offset: usize) {
        self.inner_alloc.put_all_unmanaged(src, offset)
    }
    fn put_all_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) -> RdmaHandle<T> {
        self.inner_alloc
            .put_all_buffer(scheduler, counters, src, offset)
    }
    fn put_all_buffer_unmanaged<T: Remote>(
        &self,
        src: impl Into<MemregionRdmaInputInner<T>>,
        offset: usize,
    ) {
        self.inner_alloc.put_all_buffer_unmanaged(src, offset)
    }
    fn get<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
    ) -> RdmaGetHandle<T> {
        self.inner_alloc.get(scheduler, counters, pe, offset)
    }
    fn blocking_get<T: Remote>(&self, scheduler: &Arc<Scheduler>, pe: usize, offset: usize) -> T {
        self.inner_alloc.blocking_get(scheduler, pe, offset)
    }
    fn get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> RdmaGetBufferHandle<T> {
        self.inner_alloc
            .get_buffer(scheduler, counters, pe, offset, len)
    }
    fn blocking_get_buffer<T: Remote>(
        &self,
        scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        len: usize,
    ) -> Vec<T> {
        self.inner_alloc
            .blocking_get_buffer(scheduler, pe, offset, len)
    }
    fn get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        counters: Option<Arc<[Arc<AMCounters>]>>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) -> RdmaGetIntoBufferHandle<T, B> {
        self.inner_alloc
            .get_into_buffer(scheduler, counters, pe, offset, dst)
    }
    fn blocking_get_into_buffer<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        scheduler: &Arc<Scheduler>,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) {
        self.inner_alloc
            .blocking_get_into_buffer(scheduler, pe, offset, dst)
    }
    fn get_into_buffer_unmanaged<T: Remote, B: AsLamellarBuffer<T>>(
        &self,
        pe: usize,
        offset: usize,
        dst: LamellarBuffer<T, B>,
    ) {
        self.inner_alloc.get_into_buffer_unmanaged(pe, offset, dst)
    }
}

// #[allow(dead_code)]
// #[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord)]
// pub(crate) enum CommAllocType {
//     RtHeap,
//     Fabric,
//     Remote,
// }
// unsafe impl Send for CommAllocType {}
// unsafe impl Sync for CommAllocType {}

#[derive(Copy, Clone, Add, Sub, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct CommAllocAddr(pub(crate) usize);

impl Into<usize> for &CommAllocAddr {
    fn into(self) -> usize {
        self.0
    }
}

impl Into<usize> for CommAllocAddr {
    fn into(self) -> usize {
        self.0
    }
}

impl Into<CommAllocAddr> for usize {
    fn into(self) -> CommAllocAddr {
        CommAllocAddr(self)
    }
}

impl std::ops::Deref for CommAllocAddr {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for CommAllocAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.0)
    }
}

impl std::fmt::LowerHex for CommAllocAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.0)
    }
}
// unsafe impl Send for CommAllocAddr {}
// unsafe impl Sync for CommAllocAddr {}

impl CommAllocAddr {
    pub(crate) unsafe fn as_ptr<T>(&self) -> *const T {
        self.0 as *const T
    }
    pub(crate) unsafe fn as_mut_ptr<T>(&self) -> *mut T {
        self.0 as *mut T
    }

    pub(crate) unsafe fn as_ref<T>(&self) -> Option<&T> {
        self.as_ptr::<T>().as_ref()
    }

    // pub(crate) unsafe fn as_mut<T>(&self) -> Option<&mut T> {
    //     self.as_mut_ptr::<T>().as_mut()
    // }
}

impl std::ops::Add<usize> for CommAllocAddr {
    type Output = CommAllocAddr;
    fn add(self, rhs: usize) -> Self::Output {
        CommAllocAddr(self.0 + rhs)
    }
}
impl std::ops::Add<usize> for &CommAllocAddr {
    type Output = CommAllocAddr;
    fn add(self, rhs: usize) -> Self::Output {
        CommAllocAddr(self.0 + rhs)
    }
}

impl std::ops::Sub<usize> for CommAllocAddr {
    type Output = CommAllocAddr;
    fn sub(self, rhs: usize) -> Self::Output {
        CommAllocAddr(self.0 - rhs)
    }
}
impl std::ops::Sub<usize> for &CommAllocAddr {
    type Output = CommAllocAddr;
    fn sub(self, rhs: usize) -> Self::Output {
        CommAllocAddr(self.0 - rhs)
    }
}

impl std::convert::AsRef<CommAllocAddr> for CommAllocAddr {
    fn as_ref(&self) -> &CommAllocAddr {
        self
    }
}

impl std::convert::AsRef<usize> for CommAllocAddr {
    fn as_ref(&self) -> &usize {
        &self.0
    }
}
