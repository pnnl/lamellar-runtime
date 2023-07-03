use crate::array::iterator::local_iterator::*;


#[doc(hidden)]
pub struct LocalIterCollectHandle<T: Dist + ArrayOps, A: From<UnsafeArray<T>> + SyncSend> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = Vec<T>>>>,
    pub(crate) distribution: Distribution,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) _phantom: PhantomData<A>,
}

impl<T: Dist + ArrayOps, A: From<UnsafeArray<T>> + SyncSend> LocalIterCollectHandle<T, A> {
    fn create_array(&self, local_vals: &Vec<T>) -> A {
        self.team.barrier();
        let local_sizes =
            UnsafeArray::<usize>::new(self.team.clone(), self.team.num_pes, Distribution::Block);
        unsafe {
            local_sizes.local_as_mut_slice()[0] = local_vals.len();
        }
        local_sizes.barrier();
        // local_sizes.print();
        let mut size = 0;
        let mut my_start = 0;
        let my_pe = self.team.team_pe.expect("pe not part of team");
        // local_sizes.print();
        unsafe {
            local_sizes
                .onesided_iter()
                .into_iter()
                .enumerate()
                .for_each(|(i, local_size)| {
                    size += local_size;
                    if i < my_pe {
                        my_start += local_size;
                    }
                });
        }
        // println!("my_start {} size {}", my_start, size);
        let array = UnsafeArray::<T>::new(self.team.clone(), size, self.distribution); //implcit barrier

        // safe because only a single reference to array on each PE
        // we calculate my_start so that each pes local vals are guaranteed to not overwrite another pes values.
        unsafe { array.put(my_start, local_vals) };
        array.into()
    }
}
#[async_trait]
impl<T: Dist + ArrayOps, A: From<UnsafeArray<T>> + SyncSend> LocalIterRequest
    for LocalIterCollectHandle<T, A>
{
    type Output = A;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let mut num_local_vals = 0;
        let temp_vals = vec![];
        for req in self.reqs.drain(0..) {
            let v = req.into_future().await;
            num_local_vals += v.len();
            temp_vals.push(v);
        }
        let mut local_vals = Vec::with_capacity(num_local_vals);
        unsafe{ local_vals.set_len(num_local_vals);}
        for vals in temp_vals.drain(0..){
            for (i,val) in vals.drain(0..){
                local_vals[i]=val;
            }
        }
        self.create_array(&local_vals)
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        let mut num_local_vals = 0;
        let temp_vals = vec![];
        for req in self.reqs.drain(0..) {
            let v = req.get();
            num_local_vals += v.len();
            temp_vals.push(v);
        }
        let mut local_vals = Vec::with_capacity(num_local_vals);
        unsafe{ local_vals.set_len(num_local_vals);}
        for vals in temp_vals.drain(0..){
            for (i,val) in vals.drain(0..){
                local_vals[i]=val;
            }
        }
        self.create_array(&local_vals)
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct CollectAm<I>
where
    I: LocalIterator,
{
    pub(crate) schedule: IterSchedule<I>,
}

// impl<I> std::fmt::Debug for LocalCollect<I>
// where
//     I: LocalIterator,
// {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "Collect {{   start_i: {:?}, end_i: {:?} }}",
//             self.start_i, self.end_i
//         )
//     }
// }

#[lamellar_impl::rt_am_local]
impl<I> LamellarAm for CollectAm<I>
where
    I: LocalIterator + 'static,
    I::Item: Sync,
{
    async fn exec(&self) -> Vec<(usize,I::Item)> {
        let mut iter = self.schedule.enumerate_iter()
        let mut vec = Vec::new();
        while let Some(i,elem) = iter.next() {
            vec.push((i,elem));
        }
        vec
    }
}

// #[lamellar_impl::AmLocalDataRT(Clone, Debug)]
// pub(crate) struct LocalCollectAsync<I, T>
// where
//     I: LocalIterator,
//     I::Item: Future<Output = T>,
//     T: Dist,
// {
//     pub(crate) data: I,
//     pub(crate) start_i: usize,
//     pub(crate) end_i: usize,
//     pub(crate) _phantom: PhantomData<T>,
// }

// #[lamellar_impl::rt_am_local]
// impl<I, T> LamellarAm for LocalCollectAsync<I, T, Fut>
// where
//     I: LocalIterator + 'static,
//     I::Item: Future<Output = T> + Send,
//     T: Dist,
// {
//     async fn exec(&self) -> Vec<<I::Item as Future>::Output> {
//         let mut iter = self.data.init(self.start_i, self.end_i - self.start_i);
//         let mut vec = Vec::new();
//         while let Some(elem) = iter.next() {
//             let res = elem.await;
//             vec.push(res);
//         }
//         vec
//     }
// }

