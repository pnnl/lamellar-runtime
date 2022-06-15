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

    fn for_each<I, F>(&self, iter: &I, op: F) -> Box<dyn DistIterRequest<Output = ()>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + AmLocal  + Clone + 'static,
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
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                reqs.push(self.inner.data.task_group.exec_am_local(ForEach {
                    op: op.clone(),
                    data: iter.clone(),
                    start_i: start_i,
                    end_i: end_i,
                },));
                worker += 1;
            }
        }
        Box::new(DistIterForEachHandle{ //TODO actually hold the reqs from the exec_am_local...
            reqs: reqs
        })
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F) -> Box<dyn DistIterRequest<Output = ()>>
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + AmLocal + Clone +  'static,
        Fut: Future<Output = ()> + Send +  'static,
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
            
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                reqs.push(self.inner.data.task_group.exec_am_local(
                    ForEachAsync {
                        op: op.clone(),
                        data: iter.clone(),
                        start_i: start_i,
                        end_i: end_i,
                    },
                ));
                worker += 1;
            }
        }
        Box::new(DistIterForEachHandle{ //TODO actually hold the reqs from the exec_am_local...
            reqs: reqs
        })
    }

    fn collect<I,A>(&self, iter: &I,d: Distribution) -> Box<dyn DistIterRequest<Output = A>>
        where 
        I: DistributedIterator + 'static,
        I::Item: Dist,
        A: From<UnsafeArray<I::Item>> + AmLocal + 'static
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
            while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
                let start_i = (worker as f64 * elems_per_thread).round() as usize;
                let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
                reqs.push(self.inner.data.task_group.exec_am_local(
                    Collect {
                        data: iter.clone(),
                        start_i: start_i,
                        end_i: end_i,
                    },
                ));
                worker += 1;
            }
        }
        Box::new(DistIterCollectHandle{
            reqs: reqs,
            distribution: d,
            team: self.inner.data.team.clone(),
            _phantom: PhantomData,
        })
    }

    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner.data.team.clone()
    }
}
