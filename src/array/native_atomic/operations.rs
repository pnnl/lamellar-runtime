use crate::array::native_atomic::*;
// use crate::array::native_atomic::rdma::atomic_store;
// use crate::array::operations::handle::{ArrayFetchOpHandle, BatchOpState, FetchOpState};
use crate::array::*;
// use std::collections::VecDeque;

impl<T: ElementOps + 'static> ReadOnlyOps<T> for NativeAtomicArray<T> {
    // fn load<'a>(&self, index: usize) -> ArrayFetchOpHandle<T> {
    //     if self.array.team_rt().lamellae.atomic_avail::<T>() {
    //         let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);
    //         self.network_iatomic_load(index, &buf);
    //         let team = self.team_rt();
    //         team.inc_outstanding(1);
    //         let task = self.spawn(async move {
    //             // team.lamellae.comm().wait();
    //             team.dec_outstanding(1);
    //             vec![unsafe { buf.as_slice().unwrap()[0] }]
    //         });

    //         ArrayFetchOpHandle {
    //             array: self.clone().into(),
    //             state: FetchOpState::Launched(task),
    //         }
    //     } else {
    //         let dummy_val = self.array.dummy_val(); //we dont actually do anything with this except satisfy apis;
    //         self.array
    //             .initiate_batch_fetch_op_2(dummy_val, index, ArrayOpCmd::Load, self.clone().into())
    //             .into()
    //     }
    // }
}

impl<T: ElementOps + 'static> AccessOps<T> for NativeAtomicArray<T> {
    // fn store<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
    //     // println!("in native atomic store");
    //     if self.array.team_rt().lamellae.atomic_avail::<T>() {
    //         // println!("performing network atomic");
    //         self.network_atomic_store(index, val);
    //         let mut req = VecDeque::new();
    //         let team = self.team_rt();
    //         team.inc_outstanding(1);
    //         let task = self.spawn(async move {
    //             team.lamellae.comm().wait();
    //             team.dec_outstanding(1);
    //         });
    //         req.push_back((task, vec![index]));
    //         ArrayOpHandle {
    //             array: self.clone().into(),
    //             state: BatchOpState::Launched(req),
    //         }
    //     } else {
    //         // println!("performing am atomic");
    //         self.array
    //             .initiate_batch_op(val, index, ArrayOpCmd::Store, self.clone().into())
    //             .into()
    //     }
    // }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for NativeAtomicArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for NativeAtomicArray<T> {}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for NativeAtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for NativeAtomicArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T>
    for NativeAtomicArray<T>
{
}
