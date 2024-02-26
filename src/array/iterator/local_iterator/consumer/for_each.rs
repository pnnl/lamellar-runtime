use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::local_iterator::LocalIterator;
use crate::array::iterator::{private::*, IterRequest};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::LamellarTeamRT;

use async_trait::async_trait;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ForEach<I, F>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    pub(crate) iter: I,
    pub(crate) op: F,
}

impl<I, F> IterClone for ForEach<I, F>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    fn iter_clone(&self, _: Sealed) -> Self {
        ForEach {
            iter: self.iter.iter_clone(Sealed),
            op: self.op.clone(),
        }
    }
}

impl<I, F> IterConsumer for ForEach<I, F>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    type AmOutput = ();
    type Output = ();
    type Item = I::Item;
    fn init(&self, start: usize, cnt: usize) -> Self {
        // println!("ForEach before init start {:?} cnt {:?}", start,cnt);
        let iter = ForEach {
            iter: self.iter.init(start, cnt),
            op: self.op.clone(),
        };
        // println!("ForEach after init start {:?} cnt {:?}", start,cnt);
        iter
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(ForEachAm {
            iter: self.iter_clone(Sealed),
            op: self.op.clone(),
            schedule,
        })
    }
    fn create_handle(
        self,
        _team: Pin<Arc<LamellarTeamRT>>,
        reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>,
    ) -> Box<dyn IterRequest<Output = Self::Output>> {
        Box::new(LocalIterForEachHandle { reqs })
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[derive(Debug)]
pub struct ForEachAsync<I, F, Fut>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    pub(crate) iter: I,
    pub(crate) op: F,
    // pub(crate) _phantom: PhantomData<Fut>,
}

impl<I, F, Fut> IterClone for ForEachAsync<I, F, Fut>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn iter_clone(&self, _: Sealed) -> Self {
        ForEachAsync {
            iter: self.iter.iter_clone(Sealed),
            op: self.op.clone(),
        }
    }
}

impl<I, F, Fut> IterConsumer for ForEachAsync<I, F, Fut>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    type AmOutput = ();
    type Output = ();
    type Item = I::Item;
    fn init(&self, start: usize, cnt: usize) -> Self {
        ForEachAsync {
            iter: self.iter.init(start, cnt),
            op: self.op.clone(),
        }
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(ForEachAsyncAm {
            iter: self.iter_clone(Sealed),
            op: self.op.clone(),
            schedule,
            // _phantom: self._phantom.clone(),
        })
    }
    fn create_handle(
        self,
        _team: Pin<Arc<LamellarTeamRT>>,
        reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>,
    ) -> Box<dyn IterRequest<Output = Self::Output>> {
        Box::new(LocalIterForEachHandle { reqs })
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

impl<I, F, Fut> Clone for ForEachAsync<I, F, Fut>
where
    I: LocalIterator + Clone + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn clone(&self) -> Self {
        ForEachAsync {
            iter: self.iter.clone(),
            op: self.op.clone(),
        }
    }
}

#[doc(hidden)]
pub struct LocalIterForEachHandle {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = ()>>>,
}

#[doc(hidden)]
#[async_trait]
impl IterRequest for LocalIterForEachHandle {
    type Output = ();
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(..) {
            req.into_future().await;
        }
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(..) {
            req.get();
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct ForEachAm<I, F>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    pub(crate) op: F,
    pub(crate) iter: ForEach<I, F>,
    pub(crate) schedule: IterSchedule,
}

#[lamellar_impl::rt_am_local]
impl<I, F> LamellarAm for ForEachAm<I, F>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    async fn exec(&self) {
        // println!("foreacham: {:?}", std::thread::current().id());
        let mut iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        while let Some(elem) = iter.next() {
            (&self.op)(elem);
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct ForEachAsyncAm<I, F, Fut>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    pub(crate) op: F,
    pub(crate) iter: ForEachAsync<I, F, Fut>,
    pub(crate) schedule: IterSchedule,
    // pub(crate) _phantom: PhantomData<Fut>
}

#[lamellar_impl::rt_am_local]
impl<I, F, Fut> LamellarAm for ForEachAsyncAm<I, F, Fut>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    async fn exec(&self) {
        let mut iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        while let Some(elem) = iter.next() {
            (&self.op)(elem).await;
        }
    }
}

// #[lamellar_impl::AmLocalDataRT(Clone)]
// pub(crate) struct ForEachStatic<I, F>
// where
//     I: LocalIterator,
//     F: Fn(I::Item),
// {
//     pub(crate) op: F,
//     pub(crate) data: I,
//     pub(crate) start_i: usize,
//     pub(crate) end_i: usize,
// }

// #[lamellar_impl::rt_am_local]
// impl<I, F> LamellarAm for ForEachStatic<I, F>
// where
//     I: LocalIterator + 'static,
//     F: Fn(I::Item) + SyncSend + 'static,
// {
//     async fn exec(&self) {
//         let mut iter = self.data.init(self.start_i, self.end_i - self.start_i);
//         // println!("for each static thread {:?} {} {} {}",std::thread::current().id(),self.start_i, self.end_i, self.end_i - self.start_i);
//         // let mut cnt = 0;
//         while let Some(elem) = iter.next() {
//             (&self.op)(elem);
//             // cnt += 1;
//         }
//         // println!("thread {:?} elems processed {:?}",std::thread::current().id(), cnt);
//     }
// }

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

// #[lamellar_impl::AmLocalDataRT(Clone, Debug)]
// pub(crate) struct ForEachWorkStealing<I, F>
// where
//     I: LocalIterator,
//     F: Fn(I::Item),
// {
//     pub(crate) op: F,
//     pub(crate) data: I,
//     pub(crate) range: IterWorkStealer,
//     // pub(crate) ranges: Vec<(usize, usize)>,
//     // pub(crate) range_i: Arc<AtomicUsize>,
//     pub(crate) siblings: Vec<IterWorkStealer>,
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

//-------------------------async for each-------------------------------

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
//     pub(crate) range: IterWorkStealer,
//     pub(crate) siblings: Vec<IterWorkStealer>,
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
