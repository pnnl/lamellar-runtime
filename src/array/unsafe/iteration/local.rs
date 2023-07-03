pub(crate) enum IterConsumer<I,F,R>{
    Collect,
    Count,
    ForEach(F),
    Reduce(R),
}

impl<I,F,R> IterConsumer<I,F,R> where
    I: LocalIterator + 'static,
    I::Item: SyncSend,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
    R: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,{
    
    fn into_am<A>(self, schedule: IterSchedule<I>) -> A 
    where
        A: LamellarActiveMessage + LocalAM + 'static,{
        match self {
            IterConsumer::Collect => {
                CollectAm{
                    schedule
                }
            }
            IterConsumer::Count) => {
                CountAm{
                    schedule
                }
            }
            IterConsumer::ForEach(op) => {
                ForEachAm{
                    op,
                    schedule,
                }
            }
            IterConsumer::Reduce(op) => {
                ReduceAm{
                    op,
                    schedule,
                }
            }
        }
    }
}

fn local_sched_static<I,F,R>(
    &self,
    iter: &I,
    cons: IterConsumer<I,F,R>,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
{
    let mut reqs = Vec::new();
    if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
        let num_workers = self.inner.data.team.num_threads();
        let num_elems_local = iter.elems(self.num_elems_local());
        let elems_per_thread = 1.0f64.max(num_elems_local as f64 / num_workers as f64);
        let mut worker = 0;
        let iter = iter.init(0, num_elems_local);
        while ((worker as f64 * elems_per_thread).round() as usize) < num_elems_local {
            let start_i = (worker as f64 * elems_per_thread).round() as usize;
            let end_i = ((worker + 1) as f64 * elems_per_thread).round() as usize;
            reqs.push(self.inner.data.task_group.exec_am_local_inner(
                cons.into_am(local_iterator::consumer::IterSchedule::Static(iter.clone(),start_i,end_i))
            ));
            
            worker += 1;
        }
    }
    Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
}

fn local_sched_dynamic<I, F>(
    &self,
    iter: &I,
    cons: IterConsumer<I,F,R>,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    let mut reqs = Vec::new();
    if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
        let num_workers = self.inner.data.team.num_threads();
        let num_elems_local = iter.elems(self.num_elems_local());

        let cur_i = Arc::new(AtomicUsize::new(0));
        // println!("ranges {:?}", ranges);
        for _ in 0..std::cmp::min(num_workers, num_elems_local) {
            reqs.push(self.inner.data.task_group.exec_am_local_inner(
                cons.into_am(local_iterator::consumer::IterSchedule::Dynamic(iter.clone(),cur_i.clone(),num_elems_local))
            ));
        }
    }
    Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
}

fn local_sched_work_stealing<I, F>(
    &self,
    iter: &I,
    cons: IterConsumer<I,F,R>,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    let mut reqs = Vec::new();
    if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
        let num_workers = self.inner.data.team.num_threads();
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
            siblings.push(local_iterator::consumer::IterWorkStealer {
                range: Arc::new(Mutex::new((start_i, end_i))),
            });
            worker += 1;
        }
        for sibling in &siblings {
            reqs.push(self.inner.data.task_group.exec_am_local_inner(
                cons.into_am(local_iterator::consumer::IterSchedule::WorkStealing(iter.clone(),sibling.clone(),siblings.clone()))
            ))
        }
    }
    Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
}

fn local_sched_guided<I, F>(
    &self,
    iter: &I,
    cons: IterConsumer<I,F,R>,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    let mut reqs = Vec::new();
    if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
        let num_workers = self.inner.data.team.num_threads();
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
            reqs.push(self.inner.data.task_group.exec_am_local_inner(
                cons.into_am(local_iterator::consumer::IterSchedule::Chunk(iter.clone(),ranges.clone(),range_i.clone()))
            ));
        }
    }
    Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
}

fn local_sched_chunk<I, F>(
    &self,
    iter: &I,
    cons: IterConsumer<I,F,R>,
    chunk_size: usize,
) -> Pin<Box<dyn Future<Output = ()> + Send>>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    let mut reqs = Vec::new();
    if let Ok(_my_pe) = self.inner.data.team.team_pe_id() {
        let num_workers = self.inner.data.team.num_threads();
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
            reqs.push(self.inner.data.task_group.exec_am_local_inner(
                cons.into_am(local_iterator::consumer::IterSchedule::Chunk(iter.clone(),ranges.clone(),range_i.clone()))
            ));
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
        let num_workers = self.inner.data.team.num_threads();
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
            reqs.push(self.inner.data.task_group.exec_am_local_inner(
                local_iterator::for_each::ForEachAsyncStatic {
                    op: op.clone(),
                    data: iter.clone(),
                    start_i: start_i,
                    end_i: end_i,
                },
            ));
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
        let num_workers = self.inner.data.team.num_threads();
        let num_elems_local = iter.elems(self.num_elems_local());

        let cur_i = Arc::new(AtomicUsize::new(0));
        // println!("ranges {:?}", ranges);
        for _ in 0..std::cmp::min(num_workers, num_elems_local) {
            reqs.push(self.inner.data.task_group.exec_am_local_inner(
                local_iterator::for_each::ForEachAsyncDynamic {
                    op: op.clone(),
                    data: iter.clone(),
                    cur_i: cur_i.clone(),
                    max_i: num_elems_local,
                },
            ));
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
        let num_workers = self.inner.data.team.num_threads();
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
            siblings.push(local_iterator::consumer::IterWorkStealer {
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
        let num_workers = self.inner.data.team.num_threads();
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
            reqs.push(self.inner.data.task_group.exec_am_local_inner(
                local_iterator::for_each::ForEachAsyncChunk {
                    op: op.clone(),
                    data: iter.clone(),
                    ranges: ranges.clone(),
                    range_i: range_i.clone(),
                },
            ));
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
        let num_workers = self.inner.data.team.num_threads();
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
            reqs.push(self.inner.data.task_group.exec_am_local_inner(
                local_iterator::for_each::ForEachAsyncChunk {
                    op: op.clone(),
                    data: iter.clone(),
                    ranges: ranges.clone(),
                    range_i: range_i.clone(),
                },
            ));
        }
    }
    Box::new(LocalIterForEachHandle { reqs: reqs }).into_future()
}

fn local_reduce_static<I, F>(
    &self,
    iter: &I,
    op: F,
) -> Pin<Box<dyn Future<Output =I::Item > + Send>>
where
    I: LocalIterator + 'static,
    I::Item: SyncSend,
    F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
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
            reqs.push(self.inner.data.task_group.exec_am_local_inner(
                local_iterator::reduce::ReduceStatic {
                    op: op.clone(),
                    data: iter.clone(),
                    start_i: start_i,
                    end_i: end_i,
                },
            ));
            worker += 1;
        }
    }
    Box::new(local_iterator::reduce::LocalIterReduceHandle { reqs: reqs, op: op }).into_future()
}
