use crate::array::iterator::local_iterator::*;


#[doc(hidden)]
pub struct LocalIterReduceHandle<T,F> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = T>>>,
    pub(crate) op: F,
}


#[doc(hidden)]
#[async_trait]
impl<T, F> LocalIterRequest for LocalIterReduceHandle <T,F>
where
    T: Send,
    F: Fn(T, T) -> T + SyncSend + 'static{
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
       
        futures::future::join_all(self.reqs.drain(..).map(|req| req.into_future())).await.into_iter().reduce(self.op).unwrap()
        
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        self.reqs.drain(..).map(|req| req.get()).into_iter().reduce(self.op).unwrap()
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct ReduceStatic<I, F>
// where
//     I: LocalIterator,
//     F: FnMut(I::Item, I::Item) -> I::Item,
{
    pub(crate) op: F,
    pub(crate) data: I,
    pub(crate) start_i: usize,
    pub(crate) end_i: usize,
}

#[lamellar_impl::rt_am_local]
impl<I, F> LamellarAm for ReduceStatic<I, F>
where
    I: LocalIterator + 'static,
    I::Item: SyncSend,
    F: Fn(I::Item, I::Item) -> I::Item + SyncSend + 'static,
{
    async fn exec(&self) -> I::Item{
        let mut iter = self.data.init(self.start_i, self.end_i - self.start_i);
        // println!("for each static thread {:?} {} {} {}",std::thread::current().id(),self.start_i, self.end_i, self.end_i - self.start_i);
        // let mut cnt = 0;
        let mut accum = iter.next().expect("iterator should not be empty");
        while let Some(elem) = iter.next() {
            accum =(self.op)(accum, elem);
            // cnt += 1;
        }
        accum
        // println!("thread {:?} elems processed {:?}",std::thread::current().id(), cnt);
    }
}





// we may want to support different scheduling strategies for fold
//
// #[lamellar_impl::AmLocalDataRT(Clone, Debug)]
// pub(crate) struct ForEachDynamic<I, F>
// where
//     I: LocalIterator,
//     F: Fn(I::Item),
// {
//     pub(crate) op: F,
//     pub(crate) data: I,
//     pub(crate) cur_i: Arc<AtomicUsize>,
//     pub(crate) max_i: usize,
// }

// #[lamellar_impl::rt_am_local]
// impl<I, F> LamellarAm for ForEachDynamic<I, F>
// where
//     I: LocalIterator + 'static,
//     F: Fn(I::Item) + SyncSend + 'static,
// {
//     async fn exec(&self) {
//         // println!("in for each {:?} {:?}", self.start_i, self.end_i);
//         let mut cur_i = self.cur_i.fetch_add(1, Ordering::Relaxed);

//         while cur_i < self.max_i {
//             // println!("in for each {:?} {:?} {:?}", range_i, start_i, end_i);
//             let mut iter = self.data.init(cur_i, 1);
//             while let Some(item) = iter.next() {
//                 (self.op)(item);
//             }
//             cur_i = self.cur_i.fetch_add(1, Ordering::Relaxed);
//         }
//         // println!("done in for each");
//     }
// }

// #[lamellar_impl::AmLocalDataRT(Clone, Debug)]
// pub(crate) struct ForEachChunk<I, F>
// where
//     I: LocalIterator,
//     F: Fn(I::Item),
// {
//     pub(crate) op: F,
//     pub(crate) data: I,
//     pub(crate) ranges: Vec<(usize, usize)>,
//     pub(crate) range_i: Arc<AtomicUsize>,
// }

// #[lamellar_impl::rt_am_local]
// impl<I, F> LamellarAm for ForEachChunk<I, F>
// where
//     I: LocalIterator + 'static,
//     F: Fn(I::Item) + SyncSend + 'static,
// {
//     async fn exec(&self) {
//         // println!("in for each {:?} {:?}", self.start_i, self.end_i);
//         let mut range_i = self.range_i.fetch_add(1, Ordering::Relaxed);
//         while range_i < self.ranges.len() {
//             let (start_i, end_i) = self.ranges[range_i];
//             // println!("in for each {:?} {:?} {:?}", range_i, start_i, end_i);
//             let mut iter = self.data.init(start_i, end_i - start_i);
//             while let Some(item) = iter.next() {
//                 (self.op)(item);
//             }
//             range_i = self.range_i.fetch_add(1, Ordering::Relaxed);
//         }
//         // println!("done in for each");
//     }
// }

// #[derive(Clone, Debug)]
// pub(crate) struct ForEachWorkStealer {
//     pub(crate) range: Arc<Mutex<(usize, usize)>>, //start, end
// }

// impl ForEachWorkStealer {
//     fn set_range(&self, start: usize, end: usize) {
//         let mut range = self.range.lock();
//         range.0 = start;
//         range.1 = end;
//         // println!("{:?} set range {:?}", std::thread::current().id(), range);
//     }

//     fn next(&self) -> Option<usize> {
//         let mut range = self.range.lock();
//         range.0 += 1;
//         if range.0 <= range.1 {
//             Some(range.0)
//         } else {
//             None
//         }
//     }
//     fn set_done(&self) {
//         let mut range = self.range.lock();
//         range.0 = range.1;
//     }

//     fn steal(&self) -> Option<(usize, usize)> {
//         let mut range = self.range.lock();
//         let start = range.0;
//         let end = range.1;
//         // println!("{:?} stealing {:?}", std::thread::current().id(), range);
//         if end > start && end - start > 2 {
//             let new_end = (start + end) / 2;
//             range.1 = new_end;
//             Some((new_end, end))
//         } else {
//             None
//         }
//     }
// }

// #[lamellar_impl::AmLocalDataRT(Clone, Debug)]
// pub(crate) struct ForEachWorkStealing<I, F>
// where
//     I: LocalIterator,
//     F: Fn(I::Item),
// {
//     pub(crate) op: F,
//     pub(crate) data: I,
//     pub(crate) range: ForEachWorkStealer,
//     // pub(crate) ranges: Vec<(usize, usize)>,
//     // pub(crate) range_i: Arc<AtomicUsize>,
//     pub(crate) siblings: Vec<ForEachWorkStealer>,
// }
// #[lamellar_impl::rt_am_local]
// impl<I, F> LamellarAm for ForEachWorkStealing<I, F>
// where
//     I: LocalIterator + 'static,
//     F: Fn(I::Item) + SyncSend + 'static,
// {
//     async fn exec(&self) {
//         let (start, end) = *self.range.range.lock();
//         // println!("{:?} ForEachWorkStealing {:?} {:?}",std::thread::current().id(), start, end);
//         let mut iter = self.data.init(start, end - start);
//         while self.range.next().is_some() {
//             if let Some(elem) = iter.next() {
//                 (&self.op)(elem);
//             } else {
//                 self.range.set_done();
//             }
//         }
//         // println!("{:?} ForEachWorkStealing done with my range",std::thread::current().id());
//         let mut rng = thread_rng();
//         let mut workers = (0..self.siblings.len()).collect::<Vec<usize>>();
//         workers.shuffle(&mut rng);
//         while let Some(worker) = workers.pop() {
//             // println!("{:?} ForEachWorkStealing stealing from sibling",std::thread::current().id());
//             if let Some((start, end)) = self.siblings[worker].steal() {
//                 let mut iter = self.data.init(start, end - start);
//                 self.range.set_range(start, end);
//                 while self.range.next().is_some() {
//                     if let Some(elem) = iter.next() {
//                         (&self.op)(elem);
//                     } else {
//                         self.range.set_done();
//                     }
//                 }
//                 workers = (0..self.siblings.len()).collect::<Vec<usize>>();
//                 workers.shuffle(&mut rng);
//             }
//         }
//         // println!("{:?} ForEachWorkStealing done",std::thread::current().id());
//     }
// }

// //-------------------------async for each-------------------------------

// #[lamellar_impl::AmLocalDataRT(Clone)]
// pub(crate) struct ForEachAsyncStatic<I, F, Fut>
// where
//     I: LocalIterator,
//     F: Fn(I::Item) -> Fut + SyncSend + Clone,
//     Fut: Future<Output = ()> + Send,
// {
//     pub(crate) op: F,
//     pub(crate) data: I,
//     pub(crate) start_i: usize,
//     pub(crate) end_i: usize,
// }

// impl<I, F, Fut> std::fmt::Debug for ForEachAsyncStatic<I, F, Fut>
// where
//     I: LocalIterator,
//     F: Fn(I::Item) -> Fut + SyncSend + Clone,
//     Fut: Future<Output = ()> + Send,
// {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "ForEachAsync {{   start_i: {:?}, end_i: {:?} }}",
//             self.start_i, self.end_i
//         )
//     }
// }

// #[lamellar_impl::rt_am_local]
// impl<I, F, Fut> LamellarAm for ForEachAsyncStatic<I, F, Fut>
// where
//     I: LocalIterator + 'static,
//     F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
//     Fut: Future<Output = ()> + Send + 'static,
// {
//     async fn exec(&self) {
//         let mut iter = self.data.init(self.start_i, self.end_i - self.start_i);
//         while let Some(elem) = iter.next() {
//             (&self.op)(elem).await;
//         }
//     }
// }

// #[lamellar_impl::AmLocalDataRT(Clone, Debug)]
// pub(crate) struct ForEachAsyncDynamic<I, F, Fut>
// where
//     I: LocalIterator,
//     F: Fn(I::Item) -> Fut + SyncSend + Clone,
//     Fut: Future<Output = ()> + Send,
// {
//     pub(crate) op: F,
//     pub(crate) data: I,
//     pub(crate) cur_i: Arc<AtomicUsize>,
//     pub(crate) max_i: usize,
// }

// #[lamellar_impl::rt_am_local]
// impl<I, F, Fut> LamellarAm for ForEachAsyncDynamic<I, F, Fut>
// where
//     I: LocalIterator + 'static,
//     F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
//     Fut: Future<Output = ()> + Send + 'static,
// {
//     async fn exec(&self) {
//         // println!("in for each {:?} {:?}", self.start_i, self.end_i);
//         let mut cur_i = self.cur_i.fetch_add(1, Ordering::Relaxed);

//         while cur_i < self.max_i {
//             // println!("in for each {:?} {:?} {:?}", range_i, start_i, end_i);
//             let mut iter = self.data.init(cur_i, 1);
//             while let Some(item) = iter.next() {
//                 (self.op)(item).await;
//             }
//             cur_i = self.cur_i.fetch_add(1, Ordering::Relaxed);
//         }
//         // println!("done in for each");
//     }
// }

// #[lamellar_impl::AmLocalDataRT(Clone, Debug)]
// pub(crate) struct ForEachAsyncChunk<I, F, Fut>
// where
//     I: LocalIterator,
//     F: Fn(I::Item) -> Fut + SyncSend + Clone,
//     Fut: Future<Output = ()> + Send,
// {
//     pub(crate) op: F,
//     pub(crate) data: I,
//     pub(crate) ranges: Vec<(usize, usize)>,
//     pub(crate) range_i: Arc<AtomicUsize>,
// }

// #[lamellar_impl::rt_am_local]
// impl<I, F, Fut> LamellarAm for ForEachAsyncChunk<I, F, Fut>
// where
//     I: LocalIterator + 'static,
//     F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
//     Fut: Future<Output = ()> + Send + 'static,
// {
//     async fn exec(&self) {
//         // println!("in for each {:?} {:?}", self.start_i, self.end_i);
//         let mut range_i = self.range_i.fetch_add(1, Ordering::Relaxed);
//         while range_i < self.ranges.len() {
//             let (start_i, end_i) = self.ranges[range_i];
//             // println!("in for each {:?} {:?} {:?}", range_i, start_i, end_i);
//             let mut iter = self.data.init(start_i, end_i - start_i);
//             while let Some(item) = iter.next() {
//                 (self.op)(item).await;
//             }
//             range_i = self.range_i.fetch_add(1, Ordering::Relaxed);
//         }
//         // println!("done in for each");
//     }
// }

// #[lamellar_impl::AmLocalDataRT(Clone, Debug)]
// pub(crate) struct ForEachAsyncWorkStealing<I, F, Fut>
// where
//     I: LocalIterator,
//     F: Fn(I::Item) -> Fut + SyncSend + Clone,
//     Fut: Future<Output = ()> + Send,
// {
//     pub(crate) op: F,
//     pub(crate) data: I,
//     pub(crate) range: ForEachWorkStealer,
//     pub(crate) siblings: Vec<ForEachWorkStealer>,
// }
// #[lamellar_impl::rt_am_local]
// impl<I, F, Fut> LamellarAm for ForEachAsyncWorkStealing<I, F, Fut>
// where
//     I: LocalIterator + 'static,
//     F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
//     Fut: Future<Output = ()> + Send + 'static,
// {
//     async fn exec(&self) {
//         // println!("in for each {:?} {:?}", self.start_i, self.end_i);
//         let (start, end) = *self.range.range.lock();
//         let mut iter = self.data.init(start, end - start);
//         while self.range.next().is_some() {
//             if let Some(elem) = iter.next() {
//                 (&self.op)(elem);
//             }
//         }
//         // let mut rng = thread_rng().gen();
//         let mut workers = (0..self.siblings.len()).collect::<Vec<usize>>();
//         workers.shuffle(&mut thread_rng());
//         while let Some(worker) = workers.pop() {
//             if let Some((start, end)) = self.siblings[worker].steal() {
//                 let mut iter = self.data.init(start, end - start);
//                 self.range.set_range(start, end);
//                 while self.range.next().is_some() {
//                     if let Some(elem) = iter.next() {
//                         (&self.op)(elem).await;
//                     }
//                 }
//                 workers = (0..self.siblings.len()).collect::<Vec<usize>>();
//                 workers.shuffle(&mut thread_rng());
//             }
//         }
//         // println!("done in for each");
//     }
// }