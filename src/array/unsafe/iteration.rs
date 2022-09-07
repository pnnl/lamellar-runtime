use crate::array::r#unsafe::*;

use crate::array::iterator::distributed_iterator::*;
// {
//     DistIter, DistIterMut, DistIteratorLauncher, DistributedIterator, ForEach, ForEachAsync, DistIterForEachHandle, DistIterCollectHandle
// };
use crate::array::iterator::serial_iterator::LamellarArrayIter;
use crate::array::*;
use crate::memregion::Dist;

impl<T: Dist> UnsafeArray<T> {
    pub fn dist_iter(&self) -> DistIter<'static, T, UnsafeArray<T>> {
        DistIter::new(self.clone().into(), 0, 0)
    }

    pub fn dist_iter_mut(&self) -> DistIterMut<'static, T, UnsafeArray<T>> {
        DistIterMut::new(self.clone().into(), 0, 0)
    }

    pub fn ser_iter(&self) -> LamellarArrayIter<'_, T, UnsafeArray<T>> {
        LamellarArrayIter::new(self.clone().into(), self.inner.data.team.clone(), 1)
    }

    pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T, UnsafeArray<T>> {
        LamellarArrayIter::new(
            self.clone().into(),
            self.inner.data.team.clone(),
            std::cmp::min(buf_size, self.len()),
        )
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

    fn for_each<I, F>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + AmLocal + Clone + 'static,
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
            let mut i = 0;
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
            let mut start_i = cur_i;
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
            // let iter = iter.init(0, num_elems_local);
            // while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
            //     let start_i = (worker as f64 * elems_per_thread).round() as usize;
            //     let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
            for _ in 0..std::cmp::min(num_workers, num_elems_local_orig) {
                reqs.push(self.inner.data.task_group.exec_am_local(ForEach {
                    op: op.clone(),
                    data: iter.clone(),
                    ranges: ranges.clone(),
                    range_i: range_i.clone(),
                    // start_i: start_i,
                    // end_i: end_i,
                }));
            }
            //     worker += 1;
            // }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Pin<Box<dyn Future<Output = ()> + Send>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + AmLocal + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
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
                reqs.push(self.inner.data.task_group.exec_am_local(ForEachAsync {
                    op: op.clone(),
                    data: iter.clone(),
                    start_i: start_i,
                    end_i: end_i,
                }));
                worker += 1;
            }
        }
        Box::new(DistIterForEachHandle { reqs: reqs }).into_future()
    }

    fn collect<I, A>(&self, iter: &I, d: Distribution) -> Pin<Box<dyn Future<Output = A> + Send>>
    where
        I: DistributedIterator + 'static,
        I::Item: Dist,
        A: From<UnsafeArray<I::Item>> + AmLocal + 'static,
    {
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
                reqs.push(self.inner.data.task_group.exec_am_local(Collect {
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
        A: From<UnsafeArray<B>> + AmLocal + 'static,
    {
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
                reqs.push(self.inner.data.task_group.exec_am_local(CollectAsync {
                    data: iter.clone(),
                    start_i: start_i,
                    end_i: end_i,
                    _phantom: PhantomData,
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

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.data.team.clone()
    }
}
