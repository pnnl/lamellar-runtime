use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::distributed_iterator::{DistributedIterator, Monotonic};
use crate::array::iterator::private::*;
use crate::array::iterator::{consumer::*, IterLockFuture};
use crate::array::operations::ArrayOps;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::{AsyncTeamFrom, AsyncTeamInto, Distribution};
use crate::barrier::BarrierHandle;
use crate::lamellar_env::LamellarEnv;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;
use crate::memregion::Dist;
use crate::scheduler::LamellarTask;
use crate::warnings::RuntimeWarning;

use core::marker::PhantomData;
use futures_util::{ready, Future};
use pin_project::{pin_project, pinned_drop};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub(crate) struct Collect<I, A> {
    pub(crate) iter: Monotonic<I>,
    pub(crate) distribution: Distribution,
    pub(crate) _phantom: PhantomData<A>,
}

impl<I: InnerIter, A> InnerIter for Collect<I, A> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        Collect {
            iter: self.iter.iter_clone(Sealed),
            distribution: self.distribution.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

impl<I, A> IterConsumer for Collect<I, A>
where
    I: DistributedIterator,
    I::Item: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
{
    type AmOutput = Vec<(usize, I::Item)>;
    type Output = A;
    type Item = (usize, I::Item);
    type Handle = InnerDistIterCollectHandle<I::Item, A>;
    fn init(&self, start: usize, cnt: usize) -> Self {
        Collect {
            iter: self.iter.init(start, cnt, Sealed),
            distribution: self.distribution.clone(),
            _phantom: self._phantom.clone(),
        }
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(CollectAm {
            iter: self.iter_clone(Sealed),
            schedule,
        })
    }
    fn create_handle(
        self,
        team: Pin<Arc<LamellarTeamRT>>,
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        InnerDistIterCollectHandle {
            reqs,
            distribution: self.distribution,
            team,
            state: InnerState::ReqsPending(Vec::new()),
            spawned: false,
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[derive(Debug)]
pub(crate) struct CollectAsync<I, A, B> {
    pub(crate) iter: Monotonic<I>,
    pub(crate) distribution: Distribution,
    pub(crate) _phantom: PhantomData<(A, B)>,
}

impl<I: InnerIter, A, B> InnerIter for CollectAsync<I, A, B> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        CollectAsync {
            iter: self.iter.iter_clone(Sealed),
            distribution: self.distribution.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

impl<I, A, B> IterConsumer for CollectAsync<I, A, B>
where
    I: DistributedIterator,
    I::Item: Future<Output = B> + Send + 'static,
    B: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<B>, Distribution)> + SyncSend + Clone + 'static,
{
    type AmOutput = Vec<(usize, B)>;
    type Output = A;
    type Item = (usize, I::Item);
    type Handle = InnerDistIterCollectHandle<B, A>;
    fn init(&self, start: usize, cnt: usize) -> Self {
        CollectAsync {
            iter: self.iter.init(start, cnt, Sealed),
            distribution: self.distribution.clone(),
            _phantom: self._phantom.clone(),
        }
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(CollectAsyncAm {
            iter: self.iter_clone(Sealed),
            schedule,
        })
    }
    fn create_handle(
        self,
        team: Pin<Arc<LamellarTeamRT>>,
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        InnerDistIterCollectHandle {
            reqs,
            distribution: self.distribution,
            team,
            state: InnerState::ReqsPending(Vec::new()),
            spawned: false,
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

impl<I, A, B> Clone for CollectAsync<I, A, B>
where
    I: DistributedIterator + Clone,
    I::Item: Future<Output = B> + Send + 'static,
    B: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<B>, Distribution)> + SyncSend + Clone + 'static,
{
    fn clone(&self) -> Self {
        CollectAsync {
            iter: self.iter.clone(),
            distribution: self.distribution.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

#[pin_project]
pub(crate) struct InnerDistIterCollectHandle<T, A> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<Vec<(usize, T)>>>,
    pub(crate) distribution: Distribution,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    spawned: bool,
    state: InnerState<T, A>,
}

enum InnerState<T, A> {
    ReqsPending(Vec<(usize, T)>),
    Collecting(Pin<Box<dyn Future<Output = A> + Send>>),
}

impl<T: Dist + ArrayOps, A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + 'static>
    InnerDistIterCollectHandle<T, A>
{
    async fn async_create_array(
        local_vals: Vec<T>,
        dist: Distribution,
        team: Pin<Arc<LamellarTeamRT>>,
    ) -> A {
        let input = (local_vals, dist);
        let array: A = AsyncTeamInto::team_into(input, &team.team()).await;
        array
    }

    // fn create_array(&self, local_vals: Vec<T>) -> A {
    //     let input = (local_vals, self.distribution);
    //     let array: A = TeamInto::team_into(input, &self.team);
    //     array
    // }
}

impl<T: Dist + ArrayOps, A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + 'static> Future
    for InnerDistIterCollectHandle<T, A>
{
    type Output = A;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            for req in self.reqs.iter_mut() {
                req.ready_or_set_waker(cx.waker());
            }
            self.spawned = true;
        }
        let mut this = self.project();
        match &mut this.state {
            InnerState::ReqsPending(ref mut vals) => {
                while let Some(mut req) = this.reqs.pop_front() {
                    if req.ready_or_set_waker(cx.waker()) {
                        vals.extend(req.val());
                    } else {
                        //still need to wait on this req
                        this.reqs.push_front(req);
                        return Poll::Pending;
                    }
                }
                vals.sort_by(|a, b| a.0.cmp(&b.0));
                let local_vals = vals.into_iter().map(|v| v.1).collect();
                let mut collect = Box::pin(Self::async_create_array(
                    local_vals,
                    *this.distribution,
                    this.team.clone(),
                ));

                match Future::poll(collect.as_mut(), cx) {
                    Poll::Ready(a) => {
                        return Poll::Ready(a);
                    }
                    Poll::Pending => {
                        *this.state = InnerState::Collecting(collect);
                        return Poll::Pending;
                    }
                }
            }
            InnerState::Collecting(collect) => {
                let a = ready!(Future::poll(collect.as_mut(), cx));
                Poll::Ready(a)
            }
        }
    }
}

#[pin_project(PinnedDrop)]
pub struct DistIterCollectHandle<T, A> {
    array: UnsafeArrayInner,
    launched: bool,
    #[pin]
    state: State<T, A>,
}

#[pinned_drop]
impl<T, A> PinnedDrop for DistIterCollectHandle<T, A> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            let mut this = self.project();
            RuntimeWarning::disable_warnings();
            *this.state = State::Dropped;
            RuntimeWarning::enable_warnings();
            RuntimeWarning::DroppedHandle("a DistIterCollectHandle").print();
        }
    }
}

impl<T, A> DistIterCollectHandle<T, A>
where
    T: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + 'static,
{
    pub(crate) fn new(
        lock: Option<IterLockFuture>,
        inner: Pin<Box<dyn Future<Output = InnerDistIterCollectHandle<T, A>> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        let state = match lock {
            Some(inner_lock) => State::Lock(inner_lock, Some(inner)),
            None => State::Barrier(array.barrier_handle(), inner),
        };
        Self {
            array: array.clone(),
            launched: false,
            state,
        }
    }

    /// This method will block until the associated Collect operation completes and returns the result
    pub fn block(mut self) -> A {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "DistIterCollectHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.array.clone().block_on(self)
    }

    /// This method will spawn the associated Collect Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<A> {
        self.launched = true;
        self.array.clone().spawn(self)
    }
}

#[pin_project(project = StateProj)]
enum State<T, A> {
    Lock(
        #[pin] IterLockFuture,
        Option<Pin<Box<dyn Future<Output = InnerDistIterCollectHandle<T, A>> + Send>>>,
    ),
    Barrier(
        #[pin] BarrierHandle,
        Pin<Box<dyn Future<Output = InnerDistIterCollectHandle<T, A>> + Send>>,
    ),
    Reqs(#[pin] InnerDistIterCollectHandle<T, A>),
    Dropped,
}
impl<T, A> Future for DistIterCollectHandle<T, A>
where
    T: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + 'static,
{
    type Output = A;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.state.as_mut().project() {
            StateProj::Lock(lock, inner) => {
                ready!(lock.poll(cx));
                let barrier = this.array.barrier_handle();
                *this.state = State::Barrier(
                    barrier,
                    inner.take().expect("reqs should still be in this state"),
                );
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            StateProj::Barrier(barrier, inner) => {
                ready!(barrier.poll(cx));
                let mut inner = ready!(Future::poll(inner.as_mut(), cx));
                match Pin::new(&mut inner).poll(cx) {
                    Poll::Ready(val) => Poll::Ready(val),
                    Poll::Pending => {
                        *this.state = State::Reqs(inner);
                        Poll::Pending
                    }
                }
            }
            StateProj::Reqs(inner) => {
                let val = ready!(inner.poll(cx));
                Poll::Ready(val)
            }
            StateProj::Dropped => panic!("called `Future::poll()` on a future that was dropped"),
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct CollectAm<I, A> {
    pub(crate) iter: Collect<I, A>,
    pub(crate) schedule: IterSchedule,
}

#[lamellar_impl::rt_am_local]
impl<I, A> LamellarAm for CollectAm<I, A>
where
    I: DistributedIterator,
    I::Item: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
{
    async fn exec(&self) -> Vec<(usize, I::Item)> {
        let iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        iter.collect::<Vec<_>>()
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct CollectAsyncAm<I, A, B>
where
    I: DistributedIterator,
    I::Item: Future<Output = B> + Send + 'static,
    B: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<B>, Distribution)> + SyncSend + Clone + 'static,
{
    pub(crate) iter: CollectAsync<I, A, B>,
    pub(crate) schedule: IterSchedule,
}

#[lamellar_impl::rt_am_local]
impl<I, A, B> LamellarAm for CollectAsyncAm<I, A, B>
where
    I: DistributedIterator,
    I::Item: Future<Output = B> + Send + 'static,
    B: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<B>, Distribution)> + SyncSend + Clone + 'static,
{
    async fn exec(&self) -> Vec<(usize, B)> {
        let mut iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        let mut res = vec![];
        while let Some((index, elem)) = iter.next() {
            res.push((index, elem.await));
        }
        res
    }
}
