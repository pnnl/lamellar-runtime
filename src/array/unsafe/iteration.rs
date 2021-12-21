use crate::array::r#unsafe::*;

use crate::array::iterator::distributed_iterator::{
    DistIter, DistIterMut, DistIteratorLauncher, DistributedIterator, ForEach, ForEachAsync,
};
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
        LamellarArrayIter::new(self.clone().into(), self.inner.team.clone(), 1)
    }

    pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T, UnsafeArray<T>> {
        LamellarArrayIter::new(
            self.clone().into(),
            self.inner.team.clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }
}

impl<T: Dist> DistIteratorLauncher for UnsafeArray<T> {
    fn global_index_from_local(&self, index: usize, chunk_size: usize) -> usize {
        // println!("global index cs:{:?}",chunk_size);
        let my_pe = self.inner.my_pe;
        if chunk_size == 1 {
            match self.distribution {
                Distribution::Block => {
                    let pe_start_index = (self.elem_per_pe * my_pe as f64).round() as usize;
                    index + pe_start_index
                }
                Distribution::Cyclic => self.inner.team.num_pes() * index + my_pe,
            }
        } else {
            match self.distribution {
                Distribution::Block => {
                    let num_chunks_per_per = self.elem_per_pe / chunk_size as f64;
                    // println!("{:?} {:?}", self.elem_per_pe,num_chunks_per_per);
                    let start_chunk = (num_chunks_per_per * my_pe as f64).round() as usize;
                    start_chunk + index
                }
                Distribution::Cyclic => self.inner.team.num_pes() * index + my_pe,
            }
        }
    }

    fn for_each<I, F>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) + Sync + Send + Clone + 'static,
    {
        if let Ok(_my_pe) = self.inner.team.team_pe_id() {
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
                self.inner.team.exec_am_local_tg(
                    ForEach {
                        op: op.clone(),
                        data: iter.clone(),
                        start_i: start_i,
                        end_i: end_i,
                    },
                    Some(self.inner.array_counters.clone()),
                );
                worker += 1;
            }
        }
    }
    fn for_each_async<I, F, Fut>(&self, iter: &I, op: F)
    where
        I: DistributedIterator + 'static,
        F: Fn(I::Item) -> Fut + Sync + Send + Clone + 'static,
        Fut: Future<Output = ()> + Sync + Send + Clone + 'static,
    {
        if let Ok(_my_pe) = self.inner.team.team_pe_id() {
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
                self.inner.team.exec_am_local_tg(
                    ForEachAsync {
                        op: op.clone(),
                        data: iter.clone(),
                        start_i: start_i,
                        end_i: end_i,
                    },
                    Some(self.inner.array_counters.clone()),
                );
                worker += 1;
            }
        }
    }
}
