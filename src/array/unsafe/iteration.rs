use crate::array::r#unsafe::*;

use crate::array::iterator::distributed_iterator::for_each::*;
use crate::array::iterator::distributed_iterator::*;
use crate::array::iterator::local_iterator::*;
use crate::array::iterator::local_iterator;
use crate::array::iterator::Schedule;
// {
//     DistIter, DistIterMut, DistIteratorLauncher, DistributedIterator, ForEach, ForEachAsync, DistIterForEachHandle, DistIterCollectHandle
// };
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::*;
use crate::memregion::Dist;

impl<T: Dist> UnsafeArray<T> {
    pub fn dist_iter(&self) -> DistIter<'static, T, UnsafeArray<T>> {
        DistIter::new(self.clone().into(), 0, 0)
    }

    pub fn dist_iter_mut(&self) -> DistIterMut<'static, T, UnsafeArray<T>> {
        DistIterMut::new(self.clone().into(), 0, 0)
    }

    pub fn local_iter(&self) -> LocalIter<'static, T, UnsafeArray<T>> {
        LocalIter::new(self.clone().into(), 0, 0)
    }

    pub fn local_iter_mut(&self) -> LocalIterMut<'static, T, UnsafeArray<T>> {
        LocalIterMut::new(self.clone().into(), 0, 0)
    }

    pub fn onesided_iter(&self) -> OneSidedIter<'_, T, UnsafeArray<T>> {
        OneSidedIter::new(self.clone().into(), self.inner.data.team.clone(), 1)
    }

    pub fn buffered_onesided_iter(&self, buf_size: usize) -> OneSidedIter<'_, T, UnsafeArray<T>> {
        OneSidedIter::new(
            self.clone().into(),
            self.inner.data.team.clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }

    fn for_each_static<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);

            // println!(
            //     "num_chunks {:?} chunks_thread {:?}",
            //     num_elems_local, elems_per_thread
            // );
            let mut worker = 0;
            let iter = iter.init(0, num_elems_local);
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(ForEachStatic {
                            op: op.clone(),
                            data: iter.clone(),
                            start_i: start_i,
                            end_i: end_i,
                        }),
                );
                worker += 1;
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn for_each_dynamic<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());

            let cur_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_elems_local) {
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(ForEachDynamic {
                            op: op.clone(),
                            data: iter.clone(),
                            cur_i: cur_i.clone(),
                            max_i: num_elems_local,
                        }),
                );
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn for_each_work_stealing<I, F>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);
            
            let mut worker = 0;
            let mut siblings = Vec::new();
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                siblings.push(ForEachWorkStealer {
                    range: Arc::new(Mutex::new((start_i, end_i))),
                });
                worker += 1;
            }
            println!(
                "ForEachWorkstealing Launch num_workers {num_workers} num_chunks {num_elems_local} chunks_thread {elems_per_thread} num sibs {}",siblings.len()
            );
            for sibling in &siblings {
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(ForEachWorkStealing {
                            op: op.clone(),
                            data: iter.clone(),
                            range: sibling.clone(),
                            siblings: siblings.clone(),
                        }),
                );
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn for_each_guided<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local_orig = iter.elems(self.num_elems_local());
            let mut num_elems_local = num_elems_local_orig as f64;
            let mut elems_per_thread = num_elems_local / num_workers as f64;
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
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(ForEachChunk {
                            op: op.clone(),
                            data: iter.clone(),
                            ranges: ranges.clone(),
                            range_i: range_i.clone(),
                        }),
                );
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn for_each_chunk<I, F>(
        &self,
        iter: &I,
        op: F,
        chunk_size: usize,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let mut ranges = Vec::new();
            let mut cur_i = 0;
            let mut num_chunks = 0;
            while cur_i < num_elems_local {
                ranges.push((cur_i, cur_i + chunk_size));
                cur_i += chunk_size;
                num_chunks += 1;
            }

            let range_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_chunks) {
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(ForEachChunk {
                            op: op.clone(),
                            data: iter.clone(),
                            ranges: ranges.clone(),
                            range_i: range_i.clone(),
                        }),
                );
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn for_each_async_static<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);
            // println!(
            //     "num_chunks {:?} chunks_thread {:?}",
            //     num_elems_local, elems_per_thread
            // );
            let mut worker = 0;
            let iter = iter.init(0, num_elems_local);
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(ForEachAsyncStatic {
                            op: op.clone(),
                            data: iter.clone(),
                            start_i: start_i,
                            end_i: end_i,
                        }),
                );
                worker += 1;
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn for_each_async_dynamic<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());

            let cur_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_elems_local) {
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(ForEachAsyncDynamic {
                            op: op.clone(),
                            data: iter.clone(),
                            cur_i: cur_i.clone(),
                            max_i: num_elems_local,
                        }),
                );
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn for_each_async_work_stealing<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);
            // println!(
            //     "num_chunks {:?} chunks_thread {:?}",
            //     num_elems_local, elems_per_thread
            // );
            let mut worker = 0;
            let mut siblings = Vec::new();
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                siblings.push(ForEachWorkStealer {
                    range: Arc::new(Mutex::new((start_i, end_i))),
                });
                worker += 1;
            }
            for sibling in &siblings {
                reqs.push(self.inner.data.task_group.exec_am_local_inner(
                    ForEachAsyncWorkStealing {
                        op: op.clone(),
                        data: iter.clone(),
                        range: sibling.clone(),
                        siblings: siblings.clone(),
                    },
                ));
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn for_each_async_guided<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local_orig = iter.elems(self.num_elems_local());
            let mut num_elems_local = num_elems_local_orig as f64;
            let mut elems_per_thread = num_elems_local / num_workers as f64;
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
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(ForEachAsyncChunk {
                            op: op.clone(),
                            data: iter.clone(),
                            ranges: ranges.clone(),
                            range_i: range_i.clone(),
                        }),
                );
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn for_each_async_chunk<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
        chunk_size: usize,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let mut ranges = Vec::new();
            let mut cur_i = 0;
            while cur_i < num_elems_local {
                ranges.push((cur_i, cur_i + chunk_size));
                cur_i += chunk_size;
            }

            let range_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_elems_local) {
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(ForEachAsyncChunk {
                            op: op.clone(),
                            data: iter.clone(),
                            ranges: ranges.clone(),
                            range_i: range_i.clone(),
                        }),
                );
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn local_for_each_static<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);

            // println!(
            //     "num_chunks {:?} chunks_thread {:?}",
            //     num_elems_local, elems_per_thread
            // );
            let mut worker = 0;
            let iter = iter.init(0, num_elems_local);
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(local_iterator::for_each::ForEachStatic {
                            op: op.clone(),
                            data: iter.clone(),
                            start_i: start_i,
                            end_i: end_i,
                        }),
                );
                worker += 1;
            }
        }
        Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
    }

    fn local_for_each_dynamic<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());

            let cur_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_elems_local) {
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(local_iterator::for_each::ForEachDynamic {
                            op: op.clone(),
                            data: iter.clone(),
                            cur_i: cur_i.clone(),
                            max_i: num_elems_local,
                        }),
                );
            }
        }
        Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
    }

    fn local_for_each_work_stealing<I, F>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);
            // println!(
            //     "num_chunks {:?} chunks_thread {:?}",
            //     num_elems_local, elems_per_thread
            // );
            let mut worker = 0;
            let mut siblings = Vec::new();
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                siblings.push(local_iterator::for_each::ForEachWorkStealer {
                    range: Arc::new(Mutex::new((start_i, end_i))),
                });
                worker += 1;
            }
            for sibling in &siblings {
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(local_iterator::for_each::ForEachWorkStealing {
                            op: op.clone(),
                            data: iter.clone(),
                            range: sibling.clone(),
                            siblings: siblings.clone(),
                        }),
                );
            }
        }
        Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
    }

    fn local_for_each_guided<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local_orig = iter.elems(self.num_elems_local());
            let mut num_elems_local = num_elems_local_orig as f64;
            let mut elems_per_thread = num_elems_local / num_workers as f64;
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
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(local_iterator::for_each::ForEachChunk {
                            op: op.clone(),
                            data: iter.clone(),
                            ranges: ranges.clone(),
                            range_i: range_i.clone(),
                        }),
                );
            }
        }
        Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
    }

    fn local_for_each_chunk<I, F>(
        &self,
        iter: &I,
        op: F,
        chunk_size: usize,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let mut ranges = Vec::new();
            let mut cur_i = 0;
            let mut num_chunks = 0;
            while cur_i < num_elems_local {
                ranges.push((cur_i, cur_i + chunk_size));
                cur_i += chunk_size;
                num_chunks += 1;
            }

            let range_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_chunks) {
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(local_iterator::for_each::ForEachChunk {
                            op: op.clone(),
                            data: iter.clone(),
                            ranges: ranges.clone(),
                            range_i: range_i.clone(),
                        }),
                );
            }
        }
        Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
    }

    fn local_for_each_async_static<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);
            // println!(
            //     "num_chunks {:?} chunks_thread {:?}",
            //     num_elems_local, elems_per_thread
            // );
            let mut worker = 0;
            let iter = iter.init(0, num_elems_local);
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(local_iterator::for_each::ForEachAsyncStatic {
                            op: op.clone(),
                            data: iter.clone(),
                            start_i: start_i,
                            end_i: end_i,
                        }),
                );
                worker += 1;
            }
        }
        Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
    }

    fn local_for_each_async_dynamic<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());

            let cur_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_elems_local) {
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(local_iterator::for_each::ForEachAsyncDynamic {
                            op: op.clone(),
                            data: iter.clone(),
                            cur_i: cur_i.clone(),
                            max_i: num_elems_local,
                        }),
                );
            }
        }
        Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
    }

    fn local_for_each_async_work_stealing<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);
            // println!(
            //     "num_chunks {:?} chunks_thread {:?}",
            //     num_elems_local, elems_per_thread
            // );
            let mut worker = 0;
            let mut siblings = Vec::new();
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                siblings.push(local_iterator::for_each::ForEachWorkStealer {
                    range: Arc::new(Mutex::new((start_i, end_i))),
                });
                worker += 1;
            }
            for sibling in &siblings {
                reqs.push(self.inner.data.task_group.exec_am_local_inner(
                    local_iterator::for_each::ForEachAsyncWorkStealing {
                        op: op.clone(),
                        data: iter.clone(),
                        range: sibling.clone(),
                        siblings: siblings.clone(),
                    },
                ));
            }
        }
        Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
    }

    fn local_for_each_async_guided<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local_orig = iter.elems(self.num_elems_local());
            let mut num_elems_local = num_elems_local_orig as f64;
            let mut elems_per_thread = num_elems_local / num_workers as f64;
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
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(local_iterator::for_each::ForEachAsyncChunk {
                            op: op.clone(),
                            data: iter.clone(),
                            ranges: ranges.clone(),
                            range_i: range_i.clone(),
                        }),
                );
            }
        }
        Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
    }

    fn local_for_each_async_chunk<I, F, Fut>(
        &self,
        iter: &I,
        op: F,
        chunk_size: usize,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let mut ranges = Vec::new();
            let mut cur_i = 0;
            while cur_i < num_elems_local {
                ranges.push((cur_i, cur_i + chunk_size));
                cur_i += chunk_size;
            }

            let range_i = Arc::new(AtomicUsize::new(0));
            // println!("ranges {:?}", ranges);
            for _ in 0..std::cmp::min(num_workers, num_elems_local) {
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(local_iterator::for_each::ForEachAsyncChunk {
                            op: op.clone(),
                            data: iter.clone(),
                            ranges: ranges.clone(),
                            range_i: range_i.clone(),
                        }),
                );
            }
        }
        Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
    }



}

impl<T: Dist> DistIteratorLauncher for UnsafeArray<T> {
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        // println!("global index cs:{:?}",chunk_size);
        if chunk_size == 1 {
            self.inner.global_index_from_local(index)
        } else {
            Some(self.inner.global_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    fn subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        if chunk_size == 1 {
            self.inner.subarray_index_from_local(index)
        } else {
            Some(self.inner.subarray_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    // fn subarray_pe_and_offset_for_global_index(&self, index: usize, chunk_size: usize) -> Option<(usize,usize)> {
    //     if chunk_size == 1 {
    //         Some(self.calc_pe_and_offset(index))
    //     } else {
    //         Some(self.calc_pe_and_offset(index * chunk_size)? / chunk_size)
    //     }
    // }

    fn for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        self.barrier();
        self.for_each_static(iter, op)
    }

    fn for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        self.barrier();
        match sched {
            Schedule::Static => self.for_each_static(iter, op),
            Schedule::Dynamic => self.for_each_dynamic(iter, op),
            Schedule::Chunk(size) => self.for_each_chunk(iter, op, size),
            Schedule::Guided => self.for_each_guided(iter, op),
            Schedule::WorkStealing => self.for_each_work_stealing(iter, op),
        }
    }

    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.barrier();
        self.for_each_async_static(iter, op)
    }

    fn for_each_async_with_schedule<I, F, Fut>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.barrier();
        match sched {
            Schedule::Static => self.for_each_async_static(iter, op),
            Schedule::Dynamic => self.for_each_async_dynamic(iter, op),
            Schedule::Chunk(size) => self.for_each_async_chunk(iter, op, size),
            Schedule::Guided => self.for_each_async_guided(iter, op),
            Schedule::WorkStealing => self.for_each_async_work_stealing(iter, op),
        }
    }

    fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist,
        A: From<UnsafeArray<I::Item>> + SyncSend + 'static,
    {
        self.barrier();
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = num_elems_local as f64 / num_workers as f64;
            // println!("num_chunks {:?} chunks_thread {:?}", num_elems_local, elems_per_thread);
            let mut worker = 0;
            let iter = iter.init(0, num_elems_local);
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                reqs.push(self.inner.data.task_group.exec_am_local_inner(Collect {
                    data: iter.clone(),
                    start_i: start_i,
                    end_i: end_i,
                }));
                worker += 1;
            }
        }
        Box::new(DistIterCollectHandle {
            reqs: reqs,
            distribution: d,
            team: self.inner.data.team.clone(),
            _phantom: PhantomData,
        })
        .into_future()
    }

    fn collect_async<I, A, B>(
        &self,
        iter: &I,
        d: Distribution,
    ) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Future<Output = B> + Send + 'static,
        B: Dist,
        A: From<UnsafeArray<B>> + SyncSend + 'static,
    {
        self.barrier();
        let mut reqs = Vec::new();
        if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
            let num_workers = match std::env::var("LAMELLAR_THREADS") {
                Ok(n) => n.parse::<usize>().unwrap(),
                Err(_) => 4,
            };
            let num_elems_local = iter.elems(self.num_elems_local());
            let elems_per_thread = num_elems_local as f64 / num_workers as f64;
            println!(
                "num_chunks {:?} chunks_thread {:?}",
                num_elems_local, elems_per_thread
            );
            let mut worker = 0;
            let iter = iter.init(0, num_elems_local);
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                reqs.push(
                    self.inner
                        .data
                        .task_group
                        .exec_am_local_inner(CollectAsync {
                            data: iter.clone(),
                            start_i: start_i,
                            end_i: end_i,
                            _phantom: PhantomData,
                        }),
                );
                worker += 1;
            }
        }
        Box::new(DistIterCollectHandle {
            reqs: reqs,
            distribution: d,
            team: self.inner.data.team.clone(),
            _phantom: PhantomData,
        })
        .into_future()
    }

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.data.team.clone()
    }
}


impl<T: Dist> LocalIteratorLauncher for UnsafeArray<T> {
    fn local_global_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        // println!("global index cs:{:?}",chunk_size);
        if chunk_size == 1 {
            self.inner.global_index_from_local(index)
        } else {
            Some(self.inner.global_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    fn local_subarray_index_from_local(&self, index: usize, chunk_size: usize) -> Option<usize> {
        if chunk_size == 1 {
            self.inner.subarray_index_from_local(index)
        } else {
            Some(self.inner.subarray_index_from_local(index * chunk_size)? / chunk_size)
        }
    }

    fn local_for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I:  LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        self.local_for_each_static(iter, op)
    }

    fn local_for_each_with_schedule<I, F>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I:  LocalIterator + 'static,
        F: Fn(I::Item) + SyncSend + Clone + 'static,
    {
        match sched {
            Schedule::Static => self.local_for_each_static(iter, op),
            Schedule::Dynamic => self.local_for_each_dynamic(iter, op),
            Schedule::Chunk(size) => self.local_for_each_chunk(iter, op, size),
            Schedule::Guided => self.local_for_each_guided(iter, op),
            Schedule::WorkStealing => self.local_for_each_work_stealing(iter, op),
        }
    }

    fn local_for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I:  LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.local_for_each_async_static(iter, op)
    }

    fn local_for_each_async_with_schedule<I, F, Fut>(
        &self,
        sched: Schedule,
        iter: &I,
        op: F,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I:  LocalIterator + 'static,
        F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        match sched {
            Schedule::Static => self.local_for_each_async_static(iter, op),
            Schedule::Dynamic => self.local_for_each_async_dynamic(iter, op),
            Schedule::Chunk(size) => self.local_for_each_async_chunk(iter, op, size),
            Schedule::Guided => self.local_for_each_async_guided(iter, op),
            Schedule::WorkStealing => self.local_for_each_async_work_stealing(iter, op),
        }
    }

    // fn local_collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    // where
    //     I:  LocalIterator + 'static,
    //     I::Item: Dist,
    //     A: From<UnsafeArray<I::Item>> + SyncSend + 'static,
    // {
    //     let mut reqs = Vec::new();
    //     if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
    //         let num_workers = match std::env::var("LAMELLAR_THREADS") {
    //             Ok(n) => n.parse::<usize>().unwrap(),
    //             Err(_) => 4,
    //         };
    //         let num_elems_local = iter.elems(self.num_elems_local());
    //         let elems_per_thread = num_elems_local as f64 / num_workers as f64;
    //         // println!("num_chunks {:?} chunks_thread {:?}", num_elems_local, elems_per_thread);
    //         let mut worker = 0;
    //         let iter = iter.init(0, num_elems_local);
    //         while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
    //             let start_i = (worker as f64 * elems_per_thread).round() as usize;
    //             let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
    //             reqs.push(self.inner.data.task_group.exec_am_local_inner(LocalCollect {
    //                 data: iter.clone(),
    //                 start_i: start_i,
    //                 end_i: end_i,
    //             }));
    //             worker += 1;
    //         }
    //     }
    //     Box::new(LocalIterCollectHandle {
    //         reqs: reqs,
    //         distribution: d,
    //         team: self.inner.data.team.clone(),
    //         _phantom: PhantomData,
    //     })
    //     .into_future()
    // }

    // fn local_collect_async<I, A, B>(
    //     &self,
    //     iter: &I,
    //     d: Distribution,
    // ) -> Pin<Box<dyn Future<Output = A> + Send>>
    // where
    //     I:  LocalIterator + 'static,
    //     I::Item: Future<Output = B> + Send + 'static,
    //     B: Dist,
    //     A: From<UnsafeArray<B>> + SyncSend + 'static,
    // {
    //     let mut reqs = Vec::new();
    //     if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
    //         let num_workers = match std::env::var("LAMELLAR_THREADS") {
    //             Ok(n) => n.parse::<usize>().unwrap(),
    //             Err(_) => 4,
    //         };
    //         let num_elems_local = iter.elems(self.num_elems_local());
    //         let elems_per_thread = num_elems_local as f64 / num_workers as f64;
    //         println!(
    //             "num_chunks {:?} chunks_thread {:?}",
    //             num_elems_local, elems_per_thread
    //         );
    //         let mut worker = 0;
    //         let iter = iter.init(0, num_elems_local);
    //         while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
    //             let start_i = (worker as f64 * elems_per_thread).round() as usize;
    //             let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
    //             reqs.push(
    //                 self.inner
    //                     .data
    //                     .task_group
    //                     .exec_am_local_inner(LocalCollectAsync {
    //                         data: iter.clone(),
    //                         start_i: start_i,
    //                         end_i: end_i,
    //                         _phantom: PhantomData,
    //                     }),
    //             );
    //             worker += 1;
    //         }
    //     }
    //     Box::new(LocalIterCollectHandle {
    //         reqs: reqs,
    //         distribution: d,
    //         team: self.inner.data.team.clone(),
    //         _phantom: PhantomData,
    //     })
    //     .into_future()
    // }

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.data.team.clone()
    }
}
