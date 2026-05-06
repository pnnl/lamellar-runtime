use std::sync::{Arc, atomic::AtomicUsize};

use async_std::task::yield_now;
use crate::Distribution;
use crate::scheduler::Scheduler;
use crate::{ActiveMessaging, AsLamellarBuffer, BroadcastInput, Dist, ElementArithmeticOps, ElementBitWiseOps, GenericAtomicArray, GlobalLockArray, LamellarArray, LamellarBuffer, NativeAtomicArray, ReadOnlyOps, array::NetworkAtomicArray, lamellae::{CommAlloc, CommAllocRdma, collective::{ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer}}};

pub(crate) trait AtomicArrayOpsForCollectiveOps<T: Dist>: LamellarArray<T> + ActiveMessaging + ReadOnlyOps<T> {
    fn copy_local_data(&self, index: usize, count: usize, buffer: &mut [T]);
    fn store_to_local_data(&self, index: usize, count: usize, data: &[T]);
    fn get_global_indices(&self, pe: usize, num_pes: usize, start: usize, count: usize) -> Vec<usize>;
    async fn batch_load(&self, pe: usize, num_pes: usize, start: usize, count: usize) -> Vec<T>;
}

pub(crate) trait AtomicArrayOpsForCollectiveOpsUpdate<T: ElementArithmeticOps>: AtomicArrayOpsForCollectiveOps<T> {
    fn update_local_data(&self, index: usize, count: usize, data: &[T], op: ReduceOp);
}
pub(crate) trait AtomicArrayOpsForCollectiveOpsUpdateBitwise<T: ElementBitWiseOps>: AtomicArrayOpsForCollectiveOps<T> {
    fn update_local_data_bitwise(&self, index: usize, count: usize, data: &[T], op: ReduceOp);
}

impl<T: Dist> AtomicArrayOpsForCollectiveOps<T> for GlobalLockArray<T> {
    fn copy_local_data(&self, index: usize, count: usize, buffer: &mut [T]) {
        unsafe{&mut self.array
            .local_as_mut_slice()[index..index + count]}
            .iter_mut()
            .zip(buffer.iter_mut())
            .for_each(|(elem, slot)| {
                *slot = *elem;
        });
    }

    fn store_to_local_data(&self, index: usize, count: usize, data: &[T]) {
        unsafe{&mut self.array
            .local_as_mut_slice()[index..index + count]}
            .iter_mut()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                *elem = *val;
        });
    }

    fn get_global_indices(&self, pe: usize, num_pes: usize, start: usize, count: usize) -> Vec<usize> {
        let first_global_index = self.array.first_global_index_for_pe(pe).unwrap();
        match self.array.inner.distribution {
            Distribution::Block => {
                ((start + first_global_index)..(start + first_global_index + count)).collect::<Vec<_>>()
            },
            Distribution::Cyclic => {
                (0..count).map(|i| {
                    first_global_index + (i + start) * num_pes
                }).collect::<Vec<_>>()
            },
            
        }
    }
    
    async fn batch_load(&self, pe: usize, _num_pes: usize, start: usize, count: usize) -> Vec<T> {
        unsafe {self.array.get_buffer_pe(pe, start, count).await}
    }

    
}

impl<T: ElementArithmeticOps> AtomicArrayOpsForCollectiveOpsUpdate<T> for GlobalLockArray<T> {
    fn update_local_data(&self, index: usize, count: usize, data: &[T], op: ReduceOp) {
        unsafe{&mut self.array
            .local_as_mut_slice()[index..index + count]}
            .iter_mut()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                match op {
                    ReduceOp::Sum => *elem += *val,
                    ReduceOp::Prod => *elem *= *val,
                    _ => panic!("Unsupported operation for non-bitwise update"),
                    // ReduceOp::Min => elem.fetch_min(*val),
                    // ReduceOp::Max => elem.fetch_max(*val),
                    // ReduceOp::BitOr => elem.fetch_or(*val),
                    // ReduceOp::BitXor => elem.fetch_xor(*val),
                    // ReduceOp::BitAnd => elem.fetch_and(*val),
                };
            });
    }
}

impl<T: ElementBitWiseOps> AtomicArrayOpsForCollectiveOpsUpdateBitwise<T> for GlobalLockArray<T> {
    fn update_local_data_bitwise(&self, index: usize, count: usize, data: &[T], op: ReduceOp) {
        unsafe{&mut self.array
            .local_as_mut_slice()[index..index + count]}
            .iter_mut()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                match op {
                    ReduceOp::BitOr => *elem |= *val,
                    ReduceOp::BitXor => *elem ^= *val,
                    ReduceOp::BitAnd => *elem &= *val,
                    _ => panic!("Unsupported operation for bitwise update"),
                };
            });
    }
}

impl<T: Dist> AtomicArrayOpsForCollectiveOps<T> for NetworkAtomicArray<T> {
    fn copy_local_data(&self, index: usize, count: usize, buffer: &mut [T]) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(buffer.iter_mut())
            .for_each(|(elem, slot)| {
                *slot = elem.load();
            });
    }

    fn store_to_local_data(&self, index: usize, count: usize, data: &[T]) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                elem.store(*val);
            });
    }
        
    fn get_global_indices(&self, pe: usize, num_pes: usize, start: usize, count: usize) -> Vec<usize> {
        let first_global_index = self.array.first_global_index_for_pe(pe).unwrap();
        match self.array.inner.distribution {
            Distribution::Block => {
                ((start + first_global_index)..(start + first_global_index + count)).collect::<Vec<_>>()
            },
            Distribution::Cyclic => {
                (0..count).map(|i| {
                    first_global_index + (i + start) * num_pes
                }).collect::<Vec<_>>()
            },
            
        }        
    }
    
    async fn batch_load(&self, pe: usize, num_pes: usize, start: usize, count: usize) -> Vec<T> {
        let indices = self.get_global_indices(pe, num_pes, start, count);
        ReadOnlyOps::batch_load(self, indices).await
    }

    
}

impl<T: ElementArithmeticOps> AtomicArrayOpsForCollectiveOpsUpdate<T> for NetworkAtomicArray<T> {
    fn update_local_data(&self, index: usize, count: usize, data: &[T], op: ReduceOp) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                match op {
                    ReduceOp::Sum => elem.fetch_add(*val),
                    ReduceOp::Prod => elem.fetch_mul(*val),
                    _ => panic!("Unsupported operation for non-bitwise update"),
                    // ReduceOp::Min => elem.fetch_min(*val),
                    // ReduceOp::Max => elem.fetch_max(*val),
                    // ReduceOp::BitOr => elem.fetch_or(*val),
                    // ReduceOp::BitXor => elem.fetch_xor(*val),
                    // ReduceOp::BitAnd => elem.fetch_and(*val),
                };
            });
    }
}

impl<T: ElementBitWiseOps> AtomicArrayOpsForCollectiveOpsUpdateBitwise<T> for NetworkAtomicArray<T> {
    fn update_local_data_bitwise(&self, index: usize, count: usize, data: &[T], op: ReduceOp) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                match op {
                    ReduceOp::BitOr => elem.fetch_or(*val),
                    ReduceOp::BitXor => elem.fetch_xor(*val),
                    ReduceOp::BitAnd => elem.fetch_and(*val),
                    _ => panic!("Unsupported operation for bitwise update"),
                };
            });
    }
}

impl<T: ElementBitWiseOps> AtomicArrayOpsForCollectiveOpsUpdateBitwise<T> for NativeAtomicArray<T> {
    fn update_local_data_bitwise(&self, index: usize, count: usize, data: &[T], op: ReduceOp) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                match op {
                    ReduceOp::BitOr => elem.fetch_or(*val),
                    ReduceOp::BitXor => elem.fetch_xor(*val),
                    ReduceOp::BitAnd => elem.fetch_and(*val),
                    _ => panic!("Unsupported operation for bitwise update"),
                };
            });
    }
}

impl<T: ElementBitWiseOps> AtomicArrayOpsForCollectiveOpsUpdateBitwise<T> for GenericAtomicArray<T> {
    fn update_local_data_bitwise(&self, index: usize, count: usize, data: &[T], op: ReduceOp) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                match op {
                    ReduceOp::BitOr => elem.fetch_or(*val),
                    ReduceOp::BitXor => elem.fetch_xor(*val),
                    ReduceOp::BitAnd => elem.fetch_and(*val),
                    _ => panic!("Unsupported operation for bitwise update"),
                };
            });
    }
}

impl<T: Dist > AtomicArrayOpsForCollectiveOps<T> for NativeAtomicArray<T> {
    fn copy_local_data(&self, index: usize, count: usize, buffer: &mut [T]) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(buffer.iter_mut())
            .for_each(|(elem, slot)| {
                *slot = elem.load();
            });
    }

    fn store_to_local_data(&self, index: usize, count: usize, data: &[T]) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                elem.store(*val);
            });
    }
       
    fn get_global_indices(&self, pe: usize, num_pes: usize, start: usize, count: usize) -> Vec<usize> {
        let first_global_index = self.array.first_global_index_for_pe(pe).unwrap();
        match self.array.inner.distribution {
            Distribution::Block => {
                ((start + first_global_index)..(start + first_global_index + count)).collect::<Vec<_>>()
            },
            Distribution::Cyclic => {
                (0..count).map(|i| {
                    first_global_index + (i + start) * num_pes
                }).collect::<Vec<_>>()
            },
            
        }        
    }
    
    async fn batch_load(&self, pe: usize, num_pes: usize, start: usize, count: usize) -> Vec<T> {
        let indices = self.get_global_indices(pe, num_pes, start, count);
        ReadOnlyOps::batch_load(self, indices).await
    }

    
}


impl<T: Dist + ElementArithmeticOps> AtomicArrayOpsForCollectiveOpsUpdate<T> for NativeAtomicArray<T> {
    fn update_local_data(&self, index: usize, count: usize, data: &[T], op: ReduceOp) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                match op {
                    ReduceOp::Sum => elem.fetch_add(*val),
                    ReduceOp::Prod => elem.fetch_mul(*val),
                    // ReduceOp::Min => elem.fetch_min(*val),
                    // ReduceOp::Max => elem.fetch_max(*val),
                    _ => panic!("Unsupported operation for non-bitwise update"),
                };
            });
    }
}

impl<T: Dist> AtomicArrayOpsForCollectiveOps<T> for GenericAtomicArray<T> {
    fn copy_local_data(&self, index: usize, count: usize, buffer: &mut [T]) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(buffer.iter_mut())
            .for_each(|(elem, slot)| {
                *slot = elem.load();
            });
    }

    fn store_to_local_data(&self, index: usize, count: usize, data: &[T]) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                elem.store(*val);
            });
    }
       
    fn get_global_indices(&self, pe: usize, num_pes: usize, start: usize, count: usize) -> Vec<usize> {
        let first_global_index = self.array.first_global_index_for_pe(pe).unwrap();
        match self.array.inner.distribution {
            Distribution::Block => {
                ((start + first_global_index)..(start + first_global_index + count)).collect::<Vec<_>>()
            },
            Distribution::Cyclic => {
                (0..count).map(|i| {
                    first_global_index + (i + start) * num_pes
                }).collect::<Vec<_>>()
            },
            
        }        
    }
    
    async fn batch_load(&self, pe: usize, num_pes: usize, start: usize, count: usize) -> Vec<T> {
        let indices = self.get_global_indices(pe, num_pes, start, count);
        ReadOnlyOps::batch_load(self, indices).await
    }
    
}


impl<T: Dist + ElementArithmeticOps> AtomicArrayOpsForCollectiveOpsUpdate<T> for GenericAtomicArray<T> {
    fn update_local_data(&self, index: usize, count: usize, data: &[T], op: ReduceOp) {
        self.local_data()
            .sub_data(index, index + count)
            .iter()
            .zip(data.iter())
            .for_each(|(elem, val)| {
                match op {
                    ReduceOp::Sum => elem.fetch_add(*val),
                    ReduceOp::Prod => elem.fetch_mul(*val),
                    // ReduceOp::Min => elem.fetch_min(*val),
                    // ReduceOp::Max => elem.fetch_max(*val),
                    _ => panic!("Unsupported operation for non-bitwise update"),
                };
            });
    }
}

pub(crate) async fn do_scatter<A: AtomicArrayOpsForCollectiveOps<T> + Clone, T: Dist>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    root: usize,
) -> Vec<T>
{
    do_scatter_impl(array, &scheduler, sync_alloc, index, local_length, root).await
}

pub(crate) async fn do_scatter_in_buffer<A: AtomicArrayOpsForCollectiveOps<T> + Clone, T: Dist, B: AsLamellarBuffer<T>>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    root: usize,
    mut result: LamellarBuffer<T, B>,
)
{
    let res = do_scatter_impl(array, &scheduler, sync_alloc, index, local_length, root).await;
    result.as_mut_slice().iter_mut().zip(res.iter()).for_each(|(dst, src)| {
        *dst = *src;
    });
}

pub(crate) async fn do_scatter_impl<A, T>(
    array: A,
    scheduler: &Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    root: usize,
) -> Vec<T>
where
    A: AtomicArrayOpsForCollectiveOps<T> + Clone,
    T: Dist,
{

    let local = array.my_pe();
    let num_pes = array.num_pes();
    let count = local_length; // number of elements in the array each PE is responsible for
    unsafe{sync_alloc.as_comm_slice::<AtomicUsize>().as_mut_slice()}.iter_mut().for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    let mut comm_slice = sync_alloc.as_comm_slice::<AtomicUsize>();
    let sync_slice = unsafe { comm_slice.as_mut_slice() };
    while sync_slice[1].compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        yield_now().await; // wait for root PE to signal that we are ready to start the gather
    }
    sync_slice[0].store(index, std::sync::atomic::Ordering::SeqCst); // store index in sync array to signal to other PEs that we are ready to start the scatter
    sync_slice.iter_mut().skip(2).for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    array.async_barrier().await; // ensure all PEs have initialized their sync array

    let local_offset = (local + root) % num_pes * count;
    let start = if local == root {
        index + local_offset
    } else {
        let remote_index: usize = sync_alloc.blocking_get(scheduler, root, 0);
        remote_index + local_offset
    };
    // let indices = array.get_global_indices(root, num_pes, start, count);
    let res = AtomicArrayOpsForCollectiveOps::batch_load(&array, root, num_pes, start, count).await;
    array.async_barrier().await; // ensure all PEs have received their data before returning
    sync_slice[1].store(0, std::sync::atomic::Ordering::SeqCst); // reset sync array for next collective operation
    res
}

pub(crate) async fn do_gather_in_buffer<A: AtomicArrayOpsForCollectiveOps<T> + Clone, T: Dist, B: AsLamellarBuffer<T>>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    target: RootOrLamellarBuffer<T, B>,
)
{
    let my_pe = array.my_pe();
    match target {
        RootOrLamellarBuffer::Root(mut buffer) => {
            do_gather_impl(array, &scheduler, sync_alloc, index, local_length, my_pe, buffer.as_mut_slice()).await;
        },
        RootOrLamellarBuffer::NotRoot(root) => {
            // Some(vec![T::default(); array.num_pes() * local_length]) // allocate temporary buffer for gather
            let mut buf = Vec::new(); // non-root PEs don't need to allocate a result buffer
            do_gather_impl(array, &scheduler, sync_alloc, index, local_length, root, buf.as_mut_slice()).await;
        }
    };
}


pub(crate) async fn do_gather<A: AtomicArrayOpsForCollectiveOps<T> + Clone, T: Dist>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    root: usize,
) -> Option<Vec<T>>
{
    let my_pe = array.my_pe();
    let num_pes = array.num_pes();
    let mut res = if my_pe == root {
        vec![T::default(); num_pes * local_length] // allocate result buffer on root PE
    } else {
        Vec::new() // non-root PEs don't need to allocate a result buffer
    };
    do_gather_impl(array, &scheduler, sync_alloc, index, local_length, root, &mut res).await;

    if my_pe == root {
        Some(res)
    } else {
        None
    }
}


pub(crate) async fn do_gather_impl<A, T>(
    array: A,
    scheduler:  &Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    root: usize,
    res: &mut [T],
)
where
    A: AtomicArrayOpsForCollectiveOps<T> + Clone,
    T: Dist,
{

    let local = array.my_pe();
    let num_pes = array.num_pes();
    let count = local_length; // number of elements in the array each PE is responsible for
    let mut comm_slice = sync_alloc.as_comm_slice::<AtomicUsize>();
    let sync_slice = unsafe { comm_slice.as_mut_slice() };
    while sync_slice[1].compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        yield_now().await; // wait for root PE to signal that we are ready to start the gather
    }
    sync_slice[0].store(index, std::sync::atomic::Ordering::SeqCst); // store index in sync array to signal to other PEs that we are ready to start the gather
    sync_slice.iter_mut().skip(2).for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    array.async_barrier().await; // ensure all PEs have initialized their sync array
    // let mut result = vec![T::default(); num_pes*count]; 

    if local == root {
        for i in 0..num_pes{
            let remote_index: usize = sync_alloc.blocking_get(scheduler, i, 0);
            // let indices = array.get_global_indices(i, num_pes, remote_index, count);
            let tmp = AtomicArrayOpsForCollectiveOps::batch_load(&array, i, num_pes, remote_index, count).await;
            // let tmp = array.batch_load(indices).await;
            res[i*count..i*count + count].iter_mut().zip(tmp.iter()).for_each(|(dst, src)| {
                *dst = *src; 
            }); // copy data from neighbor into correct location in res array
        }
    }
    array.async_barrier().await; // ensure all PEs have received their data before returning
    sync_slice[1].store(0, std::sync::atomic::Ordering::SeqCst); // reset sync array for next collective operation
}

pub(crate) async fn do_all_to_all_impl<A, T>(
    array: A,
    scheduler: &Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    res: &mut [T],
)
where
    A: AtomicArrayOpsForCollectiveOps<T> + Clone,
    T: Dist,
{
    let num_pes = array.num_pes();
    let count = local_length; // number of elements in the array each PE is responsible for
    let mut comm_slice = sync_alloc.as_comm_slice::<AtomicUsize>();
    let sync_slice = unsafe { comm_slice.as_mut_slice() };
    while sync_slice[1].compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        yield_now().await; // wait for root PE to signal that we are ready to start the gather
    }
    sync_slice[0].store(index, std::sync::atomic::Ordering::SeqCst); // store index in sync array to signal to other PEs that we are ready to start the all-to-all
    sync_slice.iter_mut().skip(2).for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    array.async_barrier().await; // ensure all PEs have initialized their sync array

    for i in 0..num_pes {
        let remote_index: usize = sync_alloc.blocking_get(scheduler, i, 0);
        // let indices = array.get_global_indices(i, num_pes, remote_index, count);
        let tmp = AtomicArrayOpsForCollectiveOps::batch_load(&array, i, num_pes, remote_index, count).await;

        // let tmp = array.batch_load(indices).await;
        res[i*count..i*count + count].iter_mut().zip(tmp.iter()).for_each(|(dst, src)| {
            *dst = *src; 
        }); // copy data from neighbor into correct location in res array
    }
    array.async_barrier().await; // ensure all PEs have received their data before returning
    sync_slice[1].store(0, std::sync::atomic::Ordering::SeqCst); // reset sync array for next collective operation
}


pub(crate) async fn do_all_to_all<A: AtomicArrayOpsForCollectiveOps<T> + Clone, T: Dist>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
) -> Vec<T>
{
    let num_pes = array.num_pes();
    let mut res = vec![T::default(); num_pes * local_length]; // allocate result buffer
    do_all_to_all_impl(array, &scheduler, sync_alloc, index, local_length, &mut res).await;
    res
}

pub(crate) async fn do_all_to_all_in_buffer<A: AtomicArrayOpsForCollectiveOps<T> + Clone, T: Dist, B: AsLamellarBuffer<T>>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    mut result: LamellarBuffer<T, B>,
)
{
    do_all_to_all_impl(array, &scheduler, sync_alloc, index, local_length, &mut result.as_mut_slice()).await;
}


pub(crate) async fn do_broadcast_impl<A, T>(
    array: A,
    scheduler: &Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    root: usize,
) -> Option<Vec<T>>
where
    A: AtomicArrayOpsForCollectiveOps<T> + Clone,
    T: Dist,
{

    let local = array.my_pe();
    let count = local_length; // number of elements in the array each PE is responsible for
    let mut comm_slice = sync_alloc.as_comm_slice::<AtomicUsize>();
    let sync_slice = unsafe { comm_slice.as_mut_slice() };
    while sync_slice[1].compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        yield_now().await; // wait for root PE to signal that we are ready to start the gather
    }
    sync_slice[0].store(index, std::sync::atomic::Ordering::SeqCst); // store index in sync array to signal to other PEs that we are ready to start the broadcast
    sync_slice.iter_mut().skip(2).for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    array.async_barrier().await; // ensure all PEs have initialized their sync array

    if local != root {
        let remote_index: usize = sync_alloc.blocking_get(scheduler, root, 0);
        // let indices = array.get_global_indices(root, array.num_pes(), remote_index, count);
        let res = Some(AtomicArrayOpsForCollectiveOps::batch_load(&array, root, array.num_pes(), remote_index, count).await);
        array.async_barrier().await; // ensure all PEs have received their data before returning
        res
    }
    else {
        sync_slice[1].store(0, std::sync::atomic::Ordering::SeqCst); // reset sync array for next collective operation
        None
    }
}


pub(crate) async fn do_broadcast<A: AtomicArrayOpsForCollectiveOps<T> + Clone, T: Dist>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    root: usize,
) -> Option<Vec<T>>
{
    do_broadcast_impl(array, &scheduler, sync_alloc, index, local_length, root).await
}

pub(crate) async fn do_broadcast_in_buffer<A: AtomicArrayOpsForCollectiveOps<T> + Clone, T: Dist, B: AsLamellarBuffer<T>>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    local_length: usize,
    target: RootSrcOrLamellarBuffer<T, B>,
) {
    let my_pe = array.my_pe();
    match target {
        RootSrcOrLamellarBuffer::Root(index) =>  {
            do_broadcast_impl(array, &scheduler, sync_alloc, index, local_length, my_pe).await;
        },
        RootSrcOrLamellarBuffer::NotRoot(mut result, root) => {
            let res = do_broadcast_impl(array, &scheduler, sync_alloc, 0, local_length, root).await;

            if let Some(data) = res {
                result.as_mut_slice().iter_mut().zip(data.iter()).for_each(|(dst, src)| {
                    *dst = *src;
                });
            }
        }
    };

}

pub(crate) async fn do_all_gather<A, T>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
) -> Vec<T>
where
    A: AtomicArrayOpsForCollectiveOps<T> + Clone,
    T: Dist,
{
    let mut result = vec![T::default(); array.num_pes() * local_length];
    do_all_gather_impl(array, &scheduler, sync_alloc, index, local_length, &mut result).await;
    result
}

pub(crate) async fn do_all_gather_in_buffer<A, T, B: AsLamellarBuffer<T> >(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    mut result:  LamellarBuffer<T, B>, 
)
where
    A: AtomicArrayOpsForCollectiveOps<T> + Clone,
    T: Dist,
{
    do_all_gather_impl(array, &scheduler, sync_alloc, index, local_length, result.as_mut_slice()).await;
}

pub(crate) async fn do_all_gather_impl<A, T>(
    array: A,
    scheduler: &Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    res: &mut [T],
)
where
    A: AtomicArrayOpsForCollectiveOps<T> + Clone,
    T: Dist,
{

    let num_pes = array.num_pes();
    let count = local_length; // number of elements in the array each PE is responsible for
    
    unsafe{sync_alloc.as_comm_slice::<AtomicUsize>().as_mut_slice()}.iter_mut().for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    let mut comm_slice = sync_alloc.as_comm_slice::<AtomicUsize>();
    let sync_slice = unsafe { comm_slice.as_mut_slice() };
    while sync_slice[1].compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        yield_now().await; // wait for root PE to signal that we are ready to start the gather
    }
    sync_slice[0].store(index, std::sync::atomic::Ordering::SeqCst); // store index in sync array to signal to other PEs that we are ready to start the gather
    sync_slice.iter_mut().skip(2).for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    array.async_barrier().await; // ensure all PEs have written the index before starting the gather

    // let mut result = vec![T::default(); num_pes*count]; 
    array.async_barrier().await; // ensure all PEs have published their offsets before starting the gather
    
    for i in 0..num_pes{
        let remote_index: usize = sync_alloc.blocking_get(scheduler, i, 0);
        let tmp = AtomicArrayOpsForCollectiveOps::batch_load(&array, i, num_pes, remote_index, count).await;

        res[i*count..i*count + count].iter_mut().zip(tmp.iter()).for_each(|(dst, src)| {
            *dst = *src; 
        }); // copy data from neighbor into correct location in result array
    }
    sync_slice[1].store(0, std::sync::atomic::Ordering::SeqCst); // reset sync array for next collective operation

    array.async_barrier().await; // ensure all PEs have received their data before returning
}


fn round_up_power_of_two(mut val: usize) -> usize {
    if (val == 0) || (val & (val - 1) == 0) {
        return val; // already a power of two
    }

    val |= val >> 1;
    val |= val >> 2;
    val |= val >> 4;
    val |= val >> 8;
    val |= val >> 16;
    if usize::BITS == 64 {
        val |= val >> 32;
    }
    val + 1

}

fn round_down_power_of_two(val: usize) -> usize {
    let mut pof2 = round_up_power_of_two(val);
    if pof2 > val {
        pof2 /= 2;
    }
    pof2
}

async fn do_all_reduce_impl<A, T, F>(
    array: A,
    scheduler: &Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    apply_op: F,
    res: &mut [T],
)
where
    A: AtomicArrayOpsForCollectiveOps<T> + Clone,
    T: Dist,
    F: Fn(&A, &[T]),
{
    let my_pe = array.my_pe();
    let num_pes = array.num_pes();
    let pof2 = round_down_power_of_two(num_pes);
    let rem = num_pes - pof2;
    let local = array.my_pe();
    let mut mask = 1;
    let count = local_length; // number of elements in the array each PE is responsible for reducing

    unsafe{sync_alloc.as_comm_slice::<AtomicUsize>().as_mut_slice()}.iter_mut().for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    let mut comm_slice = sync_alloc.as_comm_slice::<AtomicUsize>();
    let sync_slice = unsafe { comm_slice.as_mut_slice() };
    while sync_slice[1].compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        yield_now().await; // wait for root PE to signal that we are ready to start the gather
    }
    sync_slice[0].store(index, std::sync::atomic::Ordering::SeqCst); // store index in sync array to signal to other PEs that we are ready to start the gather
    sync_slice.iter_mut().skip(2).for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    array.async_barrier().await; // ensure all PEs have written the index before starting the gather
    let mut my_new_id ;

    let mut replace = vec![T::default(); count];
    array.copy_local_data(index, count, &mut replace);

    let mut round = 1; 

    if local < 2 * rem {
        if local % 2 == 0 {

            sync_alloc.put_unmanaged(round, local + 1, 2 + my_pe); 
            while sync_slice[2 + local + 1].load(std::sync::atomic::Ordering::SeqCst) != round {
                yield_now().await; // wait for remote PE to signal that data is ready to move on to the next round
            }
            my_new_id = usize::MAX; // this PE is out of the all reduce
        } else {
            // receive data from local - 1 and reduce into local data
            while sync_slice[2 + local - 1].load(std::sync::atomic::Ordering::SeqCst) != round {
                yield_now().await; // wait for data to arrive
            }
            let remote_index: usize = sync_alloc.blocking_get(scheduler, local - 1, 0);
            // let first_global_index = array.first_global_index_for_pe(local - 1).unwrap();
            // let indices = ((remote_index+first_global_index)..(remote_index+first_global_index+count)).collect::<Vec<_>>();
            // let indices = array.get_global_indices(local - 1, num_pes, remote_index, count);
            let tmp = AtomicArrayOpsForCollectiveOps::batch_load(&array, local - 1, num_pes, remote_index, count).await;

            // let tmp = array.batch_load(indices).await; // copy data from local - 1 into result
            // println!("PE {} received: {:?} data from PE {}", local, tmp, local-1);
            
            // array.update_local_data(index, count, tmp.as_slice(), apply_op);
            apply_op(&array, tmp.as_slice());
            // for i in 0..count {
            //     result.fetch_add(i, tmp[i]).await; // reduce data from local - 1 with local data
            // }
            sync_alloc.put_unmanaged(round, local - 1, 2 + my_pe); // signal local - 1 that reduction is complete and they can move on to the next round
            my_new_id = local / 2;
        }
        round += 1;
    }
    else {
        my_new_id = local - rem;
        round += 1;
    }

    if my_new_id != usize::MAX {
        while mask < pof2 {
            let partner = my_new_id ^ mask;
            let partner_pe = if partner < rem {
                partner * 2 + 1
            } else {
                partner + rem
            };

            sync_alloc.put_unmanaged(round, partner_pe, 2 + my_pe); // signal partner PE that data is ready
            while sync_slice[2 + partner_pe].load(std::sync::atomic::Ordering::SeqCst) != round {
                yield_now().await; // wait for partner PE to signal that their data is ready
            }
            round += 1;
            
            // receive data from partner
            let remote_index: usize = sync_alloc.blocking_get(scheduler, partner, 0);
            let tmp = AtomicArrayOpsForCollectiveOps::batch_load(&array, partner, num_pes, remote_index, count).await;

            sync_alloc.put_unmanaged(round, partner_pe, 2 + my_pe); // signal partner PE that I'm done processing their data
            
            while sync_slice[2 + partner_pe].load(std::sync::atomic::Ordering::SeqCst) != round {
                yield_now().await; // wait for partner PE to signal that they are done processing my data
            }
            
            apply_op(&array, tmp.as_slice());
            round += 1;
            mask <<= 1;
        }
    }
    else {
        while mask < pof2 {
            mask <<= 1;
            round += 2;
        }
    }

    if local < 2 * rem {
        if local % 2 != 0 {
            // send reduced data to local - 1
            sync_alloc.put_unmanaged(round, local - 1, 2 + my_pe); // signal local - 1 that reduction is complete and they can move on to the next round
        }
        else {
            while sync_slice[2 + local + 1].load(std::sync::atomic::Ordering::SeqCst) != round {
                yield_now().await; // wait for reduced data to arrive
            }
            let remote_index: usize = sync_alloc.blocking_get(scheduler, local + 1, 0);
            // let first_global_index = array.first_global_index_for_pe(local + 1).unwrap();
            // let indices = ((remote_index+first_global_index)..(remote_index+first_global_index+count)).collect::<Vec<_>>();
            // let indices = array.get_global_indices(local + 1, array.num_pes(), remote_index, count);
            // let tmp = array.batch_load(indices).await;
            let tmp = AtomicArrayOpsForCollectiveOps::batch_load(&array, local + 1, array.num_pes(), remote_index, count).await;


            apply_op(&array, tmp.as_slice());
        }
    }

    array.async_barrier().await; // ensure all PEs have finished reduction before returning
    array.copy_local_data(index, count, res);
    array.store_to_local_data(index, count, &replace); // restore original data in case this PE needs to be used for another operation after the reduce
    sync_slice[1].store(0, std::sync::atomic::Ordering::SeqCst); // reset sync array for next collective operation
}

pub(crate) async fn do_all_reduce<A: AtomicArrayOpsForCollectiveOpsUpdate<T> + Clone, T: Dist + ElementArithmeticOps>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    op: ReduceOp,
) -> Vec<T> {
    let mut result = vec![T::default(); local_length];
    do_all_reduce_impl(array, &scheduler, sync_alloc, index, local_length, |dst, val| {
        dst.update_local_data(index, local_length, val, op.clone());
    }, &mut result)
    .await;

    result
}

pub(crate) async fn do_all_reduce_bitwise<A, T>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    op: ReduceOp,
) -> Vec<T>
where
    A: AtomicArrayOpsForCollectiveOpsUpdateBitwise<T> + Clone,
    T: Dist + ElementBitWiseOps,
{
    let mut result = vec![T::default(); local_length];
    do_all_reduce_impl(array, &scheduler, sync_alloc, index, local_length, |dst, val| {
        dst.update_local_data_bitwise(index, local_length, &val, op.clone());
    }, &mut result)
    .await;

    result
}

pub(crate) async fn do_all_reduce_in_buffer<A: AtomicArrayOpsForCollectiveOpsUpdate<T> + Clone, T: Dist + ElementArithmeticOps, B: AsLamellarBuffer<T>>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    op: ReduceOp,
    mut result: LamellarBuffer<T, B>,
)  {
    do_all_reduce_impl(array, &scheduler, sync_alloc, index, local_length, |dst, val| {
        dst.update_local_data(index, local_length, val, op.clone());
    }, result.as_mut_slice())
    .await
}

pub(crate) async fn do_all_reduce_bitwise_in_buffer<A, T, B: AsLamellarBuffer<T>>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    op: ReduceOp,
    mut result: LamellarBuffer<T, B>,
)
where
    A: AtomicArrayOpsForCollectiveOpsUpdateBitwise<T> + Clone,
    T: Dist + ElementBitWiseOps,
{
    do_all_reduce_impl(array, &scheduler, sync_alloc, index, local_length, |dst, val| {
        dst.update_local_data_bitwise(index, local_length, &val, op.clone());
    }, &mut result.as_mut_slice())
    .await
}

    

async fn do_reduce_impl<A, T, F>(
    array: A,
    scheduler: &Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    root: usize,
    apply_op: F,
    res: &mut [T],
)
where
    A: AtomicArrayOpsForCollectiveOps<T> + Clone,
    T: Dist,
    F: Copy + Fn(&A, &[T]),
{

    let my_pe = array.my_pe();
    let num_pes = array.num_pes();
    let local = array.my_pe();
    let count = local_length; // number of elements in the array each PE is responsible for reducing

    let mut comm_slice = sync_alloc.as_comm_slice::<AtomicUsize>();
    let sync_slice = unsafe { comm_slice.as_mut_slice() };
    while sync_slice[1].compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        yield_now().await; // wait for root PE to signal that we are ready to start the gather
    }
    sync_slice[0].store(index, std::sync::atomic::Ordering::SeqCst); // store index in sync array to signal to other PEs that we are ready to start the gather
    sync_slice.iter_mut().skip(2).for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    array.async_barrier().await; // ensure all PEs have written the index before starting the gather

    let mut step = 1;
    let mut replace = vec![T::default(); count];
    array.copy_local_data(index, count, &mut replace);
    let virtual_id = (local + num_pes - root) % num_pes;

    while step < num_pes {
        if virtual_id % (2*step) == 0 {
            let virtual_partner = virtual_id + step;
            if virtual_partner < num_pes {
                let partner = (virtual_partner + root) % num_pes;
                while sync_slice[2 + partner].load(std::sync::atomic::Ordering::SeqCst) != step {
                    yield_now().await; // wait for remote PE to signal that data is ready to move on to the next round
                }
                let remote_index: usize = sync_alloc.blocking_get(&scheduler, partner, 0);
                let tmp = AtomicArrayOpsForCollectiveOps::batch_load(&array, partner, num_pes, remote_index, count).await;

                apply_op(&array, tmp.as_slice());
            }
        }
        else {
            let virtual_parent = virtual_id - step;
            let partner = (virtual_parent + root) % num_pes;
            sync_alloc.put_unmanaged(step, partner, 2 + my_pe); // signal partner PE that data is ready
            break; // this PE is done after sending its data to the partner
        }
        step *= 2;
    }
    array.async_barrier().await; // ensure all PEs have finished reduction before returning
    if local == root {
        Some(array.copy_local_data(index, count, res))
    } else {
        None
    };
    array.store_to_local_data(index, count, &replace); // restore original data in case this PE needs to be used for another operation after the reduce
    sync_slice[1].store(0, std::sync::atomic::Ordering::SeqCst); // reset sync array for next collective operation

    // src.iter().zip(replace.iter()).for_each(|(dst, src)| {
    //     dst.store(*src); 
    // }); // restore original data in case this PE needs to be used for another operation after the reduce
    // res
}

pub(crate) async fn do_reduce<T: ElementArithmeticOps, A: AtomicArrayOpsForCollectiveOpsUpdate<T> + Clone>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    root: usize,
    op: ReduceOp,
) -> Option<Vec<T>> {

    let my_pe = array.my_pe();
    let mut res = if my_pe == root {
        vec![T::default(); local_length] // allocate result buffer on root PE
    } else {
        Vec::new() // non-root PEs don't need to allocate a result buffer
    };
    do_reduce_impl(array, &scheduler, sync_alloc, index, local_length, root, |dst, val| {
        dst.update_local_data(index, local_length, val, op.clone());
    }, &mut res)
    .await;

    if my_pe == root {
        Some(res)
    } else {
        None
    }
}

pub(crate) async fn do_reduce_bitwise<T: ElementBitWiseOps, A: AtomicArrayOpsForCollectiveOpsUpdateBitwise<T> + Clone>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    root: usize,
    op: ReduceOp,
) -> Option<Vec<T>>
{
    let my_pe = array.my_pe();
    let mut res = if my_pe == root {
        vec![T::default(); local_length] // allocate result buffer on root PE
    } else {
        Vec::new() // non-root PEs don't need to allocate a result buffer
    };
    do_reduce_impl(array, &scheduler, sync_alloc, index, local_length, root, |dst, val| {
        dst.update_local_data_bitwise(index, local_length, val, op.clone());
    }, &mut res)
    .await;

    if my_pe == root {
        Some(res)
    } else {
        None
    }
}

pub(crate) async fn do_reduce_in_buffer<T: ElementArithmeticOps, A: AtomicArrayOpsForCollectiveOpsUpdate<T> + Clone, B: AsLamellarBuffer<T>>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    op: ReduceOp,
    target: RootOrLamellarBuffer<T, B>,
) {
    match target {
        RootOrLamellarBuffer::Root(mut result) => {
            let root = array.my_pe();
            do_reduce_impl(array, &scheduler, sync_alloc, index, local_length, root, |dst, val| {
                dst.update_local_data(index, local_length, val, op.clone());
            }, &mut result.as_mut_slice()).await
        },
        RootOrLamellarBuffer::NotRoot(root) => {
            do_reduce_impl(array, &scheduler, sync_alloc, index, local_length, root, |dst, val| {
                dst.update_local_data(index, local_length, val, op.clone());
            }, &mut Vec::new()).await
        },
    };
}

pub(crate) async fn do_reduce_bitwise_in_buffer<T: ElementBitWiseOps, A: AtomicArrayOpsForCollectiveOpsUpdateBitwise<T> + Clone, B: AsLamellarBuffer<T>>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    op: ReduceOp,
    target: RootOrLamellarBuffer<T, B>,
) {
    match target {
        RootOrLamellarBuffer::Root(mut result) => {
            let root = array.my_pe();
            do_reduce_impl(array, &scheduler, sync_alloc, index, local_length, root, |dst, val| {
                dst.update_local_data_bitwise(index, local_length, val, op.clone());
            }, &mut result.as_mut_slice())
        }.await,
        RootOrLamellarBuffer::NotRoot(root) => {
            do_reduce_impl(array, &scheduler, sync_alloc, index, local_length, root, |dst, val| {
                dst.update_local_data_bitwise(index, local_length, val, op.clone());
            }, &mut Vec::new())
        }.await,
    };
}


async fn do_reduce_scatter_impl<T, A, F>(
    array: A,
    scheduler: &Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    apply_op: F,
) -> Vec<T>
where
    A: AtomicArrayOpsForCollectiveOps<T> + Clone,
    T: Dist,
    F: Fn(&A, &[T]),
{
    let local = array.my_pe();
    let num_pes = array.num_pes();
    let count = local_length; // number of elements in the array each PE is responsible for
    
    let mut step = 1;
    let mut replace = vec![T::default(); count];
    array.copy_local_data(index, count, &mut replace);  
    // let replace = src.iter().map(|v| v.load()).collect::<Vec<_>>();
    let root = 0;
    let virtual_id = (local + num_pes - root) % num_pes;
    let mut comm_slice = sync_alloc.as_comm_slice::<AtomicUsize>();
    let sync_slice = unsafe { comm_slice.as_mut_slice() };
    while sync_slice[1].compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        yield_now().await; // wait for root PE to signal that we are ready to start the gather
    }
    sync_slice[0].store(index, std::sync::atomic::Ordering::SeqCst); // store index in sync array to signal to other PEs that we are ready to start the reduction
    sync_slice.iter_mut().skip(2).for_each(|elem| elem.store(0, std::sync::atomic::Ordering::SeqCst)); // initialize sync array to 0
    array.async_barrier().await; // ensure all PEs have initialized their sync array


    while step < num_pes {
        if virtual_id % (2*step) == 0 {
            let virtual_partner = virtual_id + step;
            if virtual_partner < num_pes {
                let partner = (virtual_partner + root) % num_pes;
                while sync_slice[2 + partner].load(std::sync::atomic::Ordering::SeqCst) != step {
                    yield_now().await; // wait for remote PE to signal that data is ready to move on to the next round
                }
                let remote_index: usize = sync_alloc.blocking_get(scheduler, partner, 0);
                // let first_global_index = array.first_global_index_for_pe(partner).unwrap();
                // let indices = ((remote_index+first_global_index)..(remote_index+first_global_index+count)).collect::<Vec<_>>();
                // let indices = array.get_global_indices(partner, num_pes, remote_index, count);

                // let tmp = array.batch_load(indices).await; // copy data from partner into result
                let tmp = AtomicArrayOpsForCollectiveOps::batch_load(&array, partner, num_pes, remote_index, count).await;

                // println!("PE {} received: {:?} data from PE {}", local, tmp, partner);
                apply_op(&array, &tmp[..]);
            }
        }
        else {
            let virtual_parent = virtual_id - step;
            let partner = (virtual_parent + root) % num_pes;
            sync_alloc.put_unmanaged(step, partner, 2 + local); // signal partner PE that data is ready
            break; // this PE is done after sending its data to the partner
        }
        step *= 2;
    }
    
    let res = do_scatter_impl(array.clone(), scheduler, sync_alloc, index, count/num_pes, 0).await; // scatter the reduced data to all PEs


    // let res = if local == root {
    //     Some(src.local_data().iter().map(|v| v.load()).collect::<Vec<_>>())
    // } else {
    //     None
    // };
    
    array.store_to_local_data(index, count, &replace); // restore original data in case this PE needs to be used for another operation after the reduce
    sync_slice[1].store(0, std::sync::atomic::Ordering::SeqCst); // reset sync array for next collective operation
    res
}

pub(crate) async fn do_reduce_scatter<T, A>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    op: ReduceOp,
) -> Vec<T>
where
    A: AtomicArrayOpsForCollectiveOpsUpdate<T> + Clone,
    T: Dist + ElementArithmeticOps,
{
    do_reduce_scatter_impl(array, &scheduler, sync_alloc, index, local_length, |dst, data| {
        dst.update_local_data(index, local_length, data, op.clone());
    })
    .await
}

pub(crate) async fn do_reduce_scatter_bitwise<T, A>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    op: ReduceOp,
) -> Vec<T>
where
    A: AtomicArrayOpsForCollectiveOpsUpdateBitwise<T> + Clone,
    T: Dist + ElementBitWiseOps,
{
    
    do_reduce_scatter_impl(array, &scheduler, sync_alloc, index, local_length, |dst, val| {
        dst.update_local_data_bitwise(index, local_length, val, op.clone());
    })
    .await
}

pub(crate) async fn do_reduce_scatter_in_buffer<T, A, B: AsLamellarBuffer<T>>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    op: ReduceOp,
    mut result: LamellarBuffer<T, B>,
)
where
    A: AtomicArrayOpsForCollectiveOpsUpdate<T> + Clone,
    T: Dist + ElementArithmeticOps,
{
    let res = do_reduce_scatter_impl(array, &scheduler, sync_alloc, index, local_length, |dst, data| {
        dst.update_local_data(index, local_length, data, op.clone());
    })
    .await;

    result.as_mut_slice().iter_mut().zip(res.into_iter()).for_each(|(dst, src)| {
        *dst = src;
    });
}

pub(crate) async fn do_reduce_scatter_bitwise_in_buffer<T, A, B: AsLamellarBuffer<T>>(
    array: A,
    scheduler: Arc<Scheduler>,
    sync_alloc: Arc<CommAlloc>,
    index: usize,
    local_length: usize,
    op: ReduceOp,
    mut result: LamellarBuffer<T, B>,
)
where
    A: AtomicArrayOpsForCollectiveOpsUpdateBitwise<T> + Clone,
    T: Dist + ElementBitWiseOps,
{


    let res = do_reduce_scatter_impl(array, &scheduler, sync_alloc, index, local_length, |dst, val| {
        dst.update_local_data_bitwise(index, local_length, val, op.clone());
    })
    .await;

    result.as_mut_slice().iter_mut().zip(res.into_iter()).for_each(|(dst, src)| {
        *dst = src;
    });
}

