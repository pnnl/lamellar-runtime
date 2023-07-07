use crate::active_messaging::SyncSend;
use crate::array::LamellarArray;
use crate::array::iterator::consumer::*;
use crate::array::r#unsafe::UnsafeArray;

use crate::memregion::Dist;

use std::pin::Pin;
use futures::Future;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use parking_lot::Mutex;

impl<T: Dist> UnsafeArray<T> {
    pub(crate) fn sched_static<C,AmO,O,I>(
        &self,
        cons: C,
    ) -> Pin<Box<dyn Future<Output = O> + Send>>
    where
        C: IterConsumer<AmOutput=AmO, Output=O, Item=I>,
        AmO: SyncSend + 'static,
        O: SyncSend + 'static,
        // I: SyncSend + 'static,
        {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = self.inner.data.team.num_threads();
            let num_elems_local = cons.max_elems(self.num_elems_local());
            let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);
            // println!("static num_workers {:?} num_elems_local {:?} elems_per_thread {:?}", num_workers, num_elems_local, elems_per_thread);
            let mut worker = 0;
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                reqs.push(self.inner.data.task_group.exec_arc_am_local_inner(
                    cons.into_am(IterSchedule::Static(start_i,end_i))
                ));
                
                worker += 1;
            }
        }
        cons.create_handle(self.inner.data.team.clone(),reqs).into_future()
    }

    pub(crate) fn sched_dynamic<C,AmO,O,I>(
        &self,
        cons: C,
    ) -> Pin<Box<dyn Future<Output = O> + Send>>
    where
        C: IterConsumer<AmOutput=AmO, Output=O, Item=I>,
        AmO: SyncSend + 'static,
        O: SyncSend + 'static,
        // I: SyncSend + 'static,
        {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = self.inner.data.team.num_threads();
            let num_elems_local = cons.max_elems(self.num_elems_local());
            // println!("dynamic num_workers {:?} num_elems_local {:?}", num_workers, num_elems_local);

            let cur_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_elems_local) {
                reqs.push(self.inner.data.task_group.exec_arc_am_local_inner(
                    cons.into_am(IterSchedule::Dynamic(cur_i.clone(),num_elems_local))
                ));
            }
        }
        cons.create_handle(self.inner.data.team.clone(),reqs).into_future()
    }

    pub(crate) fn sched_work_stealing<C,AmO,O,I>(
        &self,
        cons: C,
    ) -> Pin<Box<dyn Future<Output = O> + Send>>
    where
        C: IterConsumer<AmOutput=AmO, Output=O, Item=I>,
        AmO: SyncSend + 'static,
        O: SyncSend + 'static,
        // I: SyncSend + 'static,
        {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = self.inner.data.team.num_threads();
            let num_elems_local = cons.max_elems(self.num_elems_local());
            let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);
            // println!("work stealing num_workers {:?} num_elems_local {:?} elems_per_thread {:?}", num_workers, num_elems_local, elems_per_thread);
            let mut worker = 0;
            let mut siblings = Vec::new();
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                siblings.push(IterWorkStealer {
                    range: Arc::new(Mutex::new((start_i, end_i))),
                });
                worker += 1;
            }
            for sibling in &siblings {
                reqs.push(self.inner.data.task_group.exec_arc_am_local_inner(
                    cons.into_am(IterSchedule::WorkStealing(sibling.clone(),siblings.clone()))
                ))
            }
        }
        cons.create_handle(self.inner.data.team.clone(),reqs).into_future()
    }

    pub(crate) fn sched_guided<C,AmO,O,I>(
        &self,
        cons: C,
    ) -> Pin<Box<dyn Future<Output = O> + Send>>
    where
        C: IterConsumer<AmOutput=AmO, Output=O, Item=I>,
        AmO: SyncSend + 'static,
        O: SyncSend + 'static,
        // I: SyncSend + 'static,
        {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = self.inner.data.team.num_threads();
            let num_elems_local_orig = cons.max_elems(self.num_elems_local());
            let mut num_elems_local = num_elems_local_orig as f64;
            let mut elems_per_thread = num_elems_local / num_workers as f64;
            // println!("guided num_workers {:?} num_elems_local_orig {:?} num_elems_local {:?} elems_per_thread {:?}", num_workers, num_elems_local_orig, num_elems_local, elems_per_thread);
            let mut ranges = Vec::new();
            let mut cur_i = 0;
            let mut i;
            while elems_per_thread > 100.0 && cur_i < num_elems_local_orig {
                num_elems_local = num_elems_local / 1.61; //golden ratio
                let start_i = cur_i;
                let end_i = std::cmp::min(
                    cur_i + num_elems_local.round() as usize,
                    num_elems_local_orig,
                );
                i = 0;
                while cur_i < end_i {
                    ranges.push((
                        start_i + (i as f64 * elems_per_thread).round() as usize,
                        start_i + ((i + 1) as f64 * elems_per_thread).round() as usize,
                    ));
                    i += 1;
                    cur_i = start_i + (i as f64 * elems_per_thread).round() as usize;
                }
                elems_per_thread = num_elems_local / num_workers as f64;
            }
            if elems_per_thread < 1.0 {
                elems_per_thread = 1.0;
            }
            i = 0;
            let start_i = cur_i;
            while cur_i < num_elems_local_orig {
                ranges.push((
                    start_i + (i as f64 * elems_per_thread).round() as usize,
                    start_i + ((i + 1) as f64 * elems_per_thread).round() as usize,
                ));
                i += 1;
                cur_i = start_i + (i as f64 * elems_per_thread).round() as usize;
            }
            let range_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_elems_local_orig) {
                reqs.push(self.inner.data.task_group.exec_arc_am_local_inner(
                    cons.into_am(IterSchedule::Chunk(ranges.clone(),range_i.clone()))
                ));
            }
        }
        cons.create_handle(self.inner.data.team.clone(),reqs).into_future()
    }

    pub(crate) fn sched_chunk<C,AmO,O,I>(
        &self,
        cons: C,
        chunk_size: usize,
    ) -> Pin<Box<dyn Future<Output = O> + Send>>
    where
        C: IterConsumer<AmOutput=AmO, Output=O, Item=I>,
        AmO: SyncSend + 'static,
        O: SyncSend + 'static,
        // I: SyncSend + 'static,
        {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = self.inner.data.team.num_threads();
            let num_elems_local = cons.max_elems(self.num_elems_local());
            let mut ranges = Vec::new();
            let mut cur_i = 0;
            let mut num_chunks = 0;
            while cur_i < num_elems_local {
                ranges.push((cur_i, cur_i + chunk_size));
                cur_i += chunk_size;
                num_chunks += 1;
            }

            // println!("chunk num_workers {:?} num_elems_local {:?} num_chunks {:?}", num_workers, num_elems_local, num_chunks);

            let range_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_chunks) {
                reqs.push(self.inner.data.task_group.exec_arc_am_local_inner(
                    cons.into_am(IterSchedule::Chunk(ranges.clone(),range_i.clone()))
                ));
            }
        }
        cons.create_handle(self.inner.data.team.clone(),reqs).into_future()
    }
}
