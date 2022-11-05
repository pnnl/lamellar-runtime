use crate::array::iterator::distributed_iterator::*;

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct ForEachStatic<I, F>
where
    I: DistributedIterator,
    F: Fn(I::Item),
{
    pub(crate) op: F,
    pub(crate) data: I,
    pub(crate) start_i: usize,
    pub(crate) end_i: usize,
}

#[lamellar_impl::rt_am_local]
impl<I, F> LamellarAm for ForEachStatic<I, F>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) + SyncSend + 'static,
{
    async fn exec(&self) {
        let mut iter = self.data.init(self.start_i, self.end_i - self.start_i);
        // println!("for each static thread {:?} {} {} {}",std::thread::current().id(),self.start_i, self.end_i, self.end_i - self.start_i);
        // let mut cnt = 0;
        while let Some(elem) = iter.next() {
            (&self.op)(elem);
            // cnt += 1;
        }
        // println!("thread {:?} elems processed {:?}",std::thread::current().id(), cnt);
    }
}

#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub(crate) struct ForEachDynamic<I, F>
where
    I: DistributedIterator,
    F: Fn(I::Item),
{
    pub(crate) op: F,
    pub(crate) data: I,
    pub(crate) cur_i: Arc<AtomicUsize>,
    pub(crate) max_i: usize,
}

#[lamellar_impl::rt_am_local]
impl<I, F> LamellarAm for ForEachDynamic<I, F>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) + SyncSend + 'static,
{
    async fn exec(&self) {
        // println!("in for each {:?} {:?}", self.start_i, self.end_i);
        let mut cur_i = self.cur_i.fetch_add(1, Ordering::Relaxed);

        while cur_i < self.max_i {
            // println!("in for each {:?} {:?} {:?}", range_i, start_i, end_i);
            let mut iter = self.data.init(cur_i, 1);
            while let Some(item) = iter.next() {
                (self.op)(item);
            }
            cur_i = self.cur_i.fetch_add(1, Ordering::Relaxed);
        }
        // println!("done in for each");
    }
}

#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub(crate) struct ForEachChunk<I, F>
where
    I: DistributedIterator,
    F: Fn(I::Item),
{
    pub(crate) op: F,
    pub(crate) data: I,
    pub(crate) ranges: Vec<(usize, usize)>,
    pub(crate) range_i: Arc<AtomicUsize>,
}

#[lamellar_impl::rt_am_local]
impl<I, F> LamellarAm for ForEachChunk<I, F>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) + SyncSend + 'static,
{
    async fn exec(&self) {
        // println!("in for each {:?} {:?}", self.start_i, self.end_i);
        let mut range_i = self.range_i.fetch_add(1, Ordering::Relaxed);
        while range_i < self.ranges.len() {
            let (start_i, end_i) = self.ranges[range_i];
            // println!("in for each {:?} {:?} {:?}", range_i, start_i, end_i);
            let mut iter = self.data.init(start_i, end_i - start_i);
            while let Some(item) = iter.next() {
                (self.op)(item);
            }
            range_i = self.range_i.fetch_add(1, Ordering::Relaxed);
        }
        // println!("done in for each");
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ForEachWorkStealer {
    pub(crate) range: Arc<Mutex<(usize, usize)>>, //start, end
}

impl ForEachWorkStealer {
    fn set_range(&self, start: usize, end: usize) {
        let mut range = self.range.lock();
        range.0 = start;
        range.1 = end;
        // println!("{:?} set range {:?}", std::thread::current().id(), range);
    }

    fn next(&self) -> Option<usize> {
        let mut range = self.range.lock();
        range.0 += 1;
        if range.0 <= range.1 {
            Some(range.0)
        } else {
            None
        }
    }

    fn steal(&self) -> Option<(usize, usize)> {
        let mut range = self.range.lock();
        let start = range.0;
        let end = range.1;
        // println!("{:?} stealing {:?}", std::thread::current().id(), range);
        if end > start && end - start > 2 {
            let new_end = (start + end) / 2;
            range.1 = new_end;
            Some((new_end, end))
        } else {
            None
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub(crate) struct ForEachWorkStealing<I, F>
where
    I: DistributedIterator,
    F: Fn(I::Item),
{
    pub(crate) op: F,
    pub(crate) data: I,
    pub(crate) range: ForEachWorkStealer,
    // pub(crate) ranges: Vec<(usize, usize)>,
    // pub(crate) range_i: Arc<AtomicUsize>,
    pub(crate) siblings: Vec<ForEachWorkStealer>,
}
#[lamellar_impl::rt_am_local]
impl<I, F> LamellarAm for ForEachWorkStealing<I, F>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) + SyncSend + 'static,
{
    async fn exec(&self) {
        // println!("in for each {:?} {:?}", self.start_i, self.end_i);
        let (start, end) = *self.range.range.lock();
        let mut iter = self.data.init(start, end - start);
        while self.range.next().is_some() {
            if let Some(elem) = iter.next() {
                (&self.op)(elem);
            }
        }
        let mut rng = thread_rng();
        let mut workers = (0..self.siblings.len()).collect::<Vec<usize>>();
        workers.shuffle(&mut rng);
        while let Some(worker) = workers.pop() {
            if let Some((start, end)) = self.siblings[worker].steal() {
                let mut iter = self.data.init(start, end - start);
                self.range.set_range(start, end);
                while self.range.next().is_some() {
                    if let Some(elem) = iter.next() {
                        (&self.op)(elem);
                    }
                }
                workers = (0..self.siblings.len()).collect::<Vec<usize>>();
                workers.shuffle(&mut rng);
            }
        }
        // println!("done in for each");
    }
}

//-------------------------async for each-------------------------------

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct ForEachAsyncStatic<I, F, Fut>
where
    I: DistributedIterator,
    F: Fn(I::Item) -> Fut + SyncSend + Clone,
    Fut: Future<Output = ()> + Send,
{
    pub(crate) op: F,
    pub(crate) data: I,
    pub(crate) start_i: usize,
    pub(crate) end_i: usize,
}

impl<I, F, Fut> std::fmt::Debug for ForEachAsyncStatic<I, F, Fut>
where
    I: DistributedIterator,
    F: Fn(I::Item) -> Fut + SyncSend + Clone,
    Fut: Future<Output = ()> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ForEachAsync {{   start_i: {:?}, end_i: {:?} }}",
            self.start_i, self.end_i
        )
    }
}

#[lamellar_impl::rt_am_local]
impl<I, F, Fut> LamellarAm for ForEachAsyncStatic<I, F, Fut>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    async fn exec(&self) {
        let mut iter = self.data.init(self.start_i, self.end_i - self.start_i);
        while let Some(elem) = iter.next() {
            (&self.op)(elem).await;
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub(crate) struct ForEachAsyncDynamic<I, F, Fut>
where
    I: DistributedIterator,
    F: Fn(I::Item) -> Fut + SyncSend + Clone,
    Fut: Future<Output = ()> + Send,
{
    pub(crate) op: F,
    pub(crate) data: I,
    pub(crate) cur_i: Arc<AtomicUsize>,
    pub(crate) max_i: usize,
}

#[lamellar_impl::rt_am_local]
impl<I, F, Fut> LamellarAm for ForEachAsyncDynamic<I, F, Fut>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    async fn exec(&self) {
        // println!("in for each {:?} {:?}", self.start_i, self.end_i);
        let mut cur_i = self.cur_i.fetch_add(1, Ordering::Relaxed);

        while cur_i < self.max_i {
            // println!("in for each {:?} {:?} {:?}", range_i, start_i, end_i);
            let mut iter = self.data.init(cur_i, 1);
            while let Some(item) = iter.next() {
                (self.op)(item).await;
            }
            cur_i = self.cur_i.fetch_add(1, Ordering::Relaxed);
        }
        // println!("done in for each");
    }
}

#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub(crate) struct ForEachAsyncChunk<I, F, Fut>
where
    I: DistributedIterator,
    F: Fn(I::Item) -> Fut + SyncSend + Clone,
    Fut: Future<Output = ()> + Send,
{
    pub(crate) op: F,
    pub(crate) data: I,
    pub(crate) ranges: Vec<(usize, usize)>,
    pub(crate) range_i: Arc<AtomicUsize>,
}

#[lamellar_impl::rt_am_local]
impl<I, F, Fut> LamellarAm for ForEachAsyncChunk<I, F, Fut>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    async fn exec(&self) {
        // println!("in for each {:?} {:?}", self.start_i, self.end_i);
        let mut range_i = self.range_i.fetch_add(1, Ordering::Relaxed);
        while range_i < self.ranges.len() {
            let (start_i, end_i) = self.ranges[range_i];
            // println!("in for each {:?} {:?} {:?}", range_i, start_i, end_i);
            let mut iter = self.data.init(start_i, end_i - start_i);
            while let Some(item) = iter.next() {
                (self.op)(item).await;
            }
            range_i = self.range_i.fetch_add(1, Ordering::Relaxed);
        }
        // println!("done in for each");
    }
}

#[lamellar_impl::AmLocalDataRT(Clone, Debug)]
pub(crate) struct ForEachAsyncWorkStealing<I, F, Fut>
where
    I: DistributedIterator,
    F: Fn(I::Item) -> Fut + SyncSend + Clone,
    Fut: Future<Output = ()> + Send,
{
    pub(crate) op: F,
    pub(crate) data: I,
    pub(crate) range: ForEachWorkStealer,
    pub(crate) siblings: Vec<ForEachWorkStealer>,
}
#[lamellar_impl::rt_am_local]
impl<I, F, Fut> LamellarAm for ForEachAsyncWorkStealing<I, F, Fut>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    async fn exec(&self) {
        // println!("in for each {:?} {:?}", self.start_i, self.end_i);
        let (start, end) = *self.range.range.lock();
        let mut iter = self.data.init(start, end - start);
        while self.range.next().is_some() {
            if let Some(elem) = iter.next() {
                (&self.op)(elem);
            }
        }
        // let mut rng = thread_rng().gen();
        let mut workers = (0..self.siblings.len()).collect::<Vec<usize>>();
        workers.shuffle(&mut thread_rng());
        while let Some(worker) = workers.pop() {
            if let Some((start, end)) = self.siblings[worker].steal() {
                let mut iter = self.data.init(start, end - start);
                self.range.set_range(start, end);
                while self.range.next().is_some() {
                    if let Some(elem) = iter.next() {
                        (&self.op)(elem).await;
                    }
                }
                workers = (0..self.siblings.len()).collect::<Vec<usize>>();
                workers.shuffle(&mut thread_rng());
            }
        }
        // println!("done in for each");
    }
}
