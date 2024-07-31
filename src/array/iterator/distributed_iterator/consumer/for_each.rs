use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::distributed_iterator::DistributedIterator;
use crate::array::iterator::private::*;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::barrier::BarrierHandle;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::LamellarTask;

use futures_util::{ready, Future};
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

#[derive(Clone, Debug)]
pub(crate) struct ForEach<I, F>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    pub(crate) iter: I,
    pub(crate) op: F,
}

impl<I, F> IterClone for ForEach<I, F>
where
    I: DistributedIterator + 'static,
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
    I: DistributedIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    type AmOutput = ();
    type Output = ();
    type Item = I::Item;
    type Handle = InnerDistIterForEachHandle;
    fn init(&self, start: usize, cnt: usize) -> Self {
        ForEach {
            iter: self.iter.init(start, cnt),
            op: self.op.clone(),
        }
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
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        InnerDistIterForEachHandle { reqs }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[derive(Debug)]
pub(crate) struct ForEachAsync<I, F, Fut>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    pub(crate) iter: I,
    pub(crate) op: F,
    // pub(crate) _phantom: PhantomData<Fut>,
}

impl<I, F, Fut> IterClone for ForEachAsync<I, F, Fut>
where
    I: DistributedIterator + 'static,
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
    I: DistributedIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    type AmOutput = ();
    type Output = ();
    type Item = I::Item;
    type Handle = InnerDistIterForEachHandle;
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
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        InnerDistIterForEachHandle { reqs }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

impl<I, F, Fut> Clone for ForEachAsync<I, F, Fut>
where
    I: DistributedIterator + Clone + 'static,
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

pub(crate) struct InnerDistIterForEachHandle {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<()>>,
}

impl Future for InnerDistIterForEachHandle {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        while let Some(mut req) = self.reqs.pop_front() {
            if !req.ready_or_set_waker(cx.waker()) {
                self.reqs.push_front(req);
                return Poll::Pending;
            }
        }
        Poll::Ready(())
    }
}

//#[doc(hidden)]
impl LamellarRequest for InnerDistIterForEachHandle {
    fn blocking_wait(mut self) -> Self::Output {
        for req in self.reqs.drain(..) {
            req.blocking_wait();
        }
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        for req in self.reqs.iter_mut() {
            if !req.ready_or_set_waker(waker) {
                //only need to wait on the next unready req
                return false;
            }
        }
        true
    }
    fn val(&self) -> Self::Output {
        for req in self.reqs.iter() {
            req.val();
        }
    }
}

//#[doc(hidden)]
#[pin_project]
pub struct DistIterForEachHandle {
    // pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<()>>,
    team: Pin<Arc<LamellarTeamRT>>,
    #[pin]
    state: State,
}

impl DistIterForEachHandle {
    pub(crate) fn new(
        barrier: BarrierHandle,
        reqs: Pin<Box<dyn Future<Output = InnerDistIterForEachHandle> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        DistIterForEachHandle {
            team: array.data.team.clone(),
            state: State::Barrier(barrier, reqs),
        }
    }

    /// This method will block until the associated For Each operation completes and returns the result
    pub fn block(self) {
        self.team.clone().block_on(self);
    }
    /// This method will spawn the associated  For Each Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<()> {
        // match self.state {
        //     State::Barrier(ref barrier, _) => {
        //         println!("spawning task barrier id {:?}", barrier.barrier_id);
        //     }
        //     State::Reqs(_, barrier_id) => {
        //         println!("spawning task not sure I can be here {:?}", barrier_id);
        //     }
        // }
        self.team.clone().scheduler.spawn_task(self)
    }
}

#[pin_project(project = StateProj)]
enum State {
    Barrier(
        #[pin] BarrierHandle,
        Pin<Box<dyn Future<Output = InnerDistIterForEachHandle> + Send>>,
    ),
    Reqs(#[pin] InnerDistIterForEachHandle, usize),
}

impl Future for DistIterForEachHandle {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match this.state.as_mut().project() {
            StateProj::Barrier(barrier, inner) => {
                let barrier_id = barrier.barrier_id;
                // println!("in task barrier {:?}", barrier_id);
                ready!(barrier.poll(cx));
                // println!("past barrier {:?}", barrier_id);
                let mut inner: InnerDistIterForEachHandle =
                    ready!(Future::poll(inner.as_mut(), cx));

                match Pin::new(&mut inner).poll(cx) {
                    Poll::Ready(()) => {
                        // println!("past reqs  barrier_id {:?}", barrier_id);
                        Poll::Ready(())
                    }
                    Poll::Pending => {
                        // println!(
                        //     "reqs remaining {:?} barrier_id {:?}",
                        //     inner.reqs.len(),
                        //     barrier_id
                        // );
                        *this.state = State::Reqs(inner, barrier_id);
                        Poll::Pending
                    }
                }
            }
            StateProj::Reqs(inner, barrier_id) => {
                // println!(
                //     "reqs remaining {:?} barrier_id {:?}",
                //     inner.reqs.len(),
                //     barrier_id
                // );
                match inner.poll(cx) {
                    Poll::Ready(()) => {
                        // println!("past reqs barrier_id {:?}", barrier_id);
                        Poll::Ready(())
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

//#[doc(hidden)]
impl LamellarRequest for DistIterForEachHandle {
    fn blocking_wait(self) -> Self::Output {
        match self.state {
            State::Barrier(barrier, reqs) => {
                barrier.blocking_wait();
                self.team.block_on(reqs).blocking_wait();
            }
            State::Reqs(inner, _) => {
                inner.blocking_wait();
            }
        }
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        match &mut self.state {
            State::Barrier(barrier, _) => {
                if !barrier.ready_or_set_waker(waker) {
                    return false;
                }
                waker.wake_by_ref();
                false
            }
            State::Reqs(inner, _) => inner.ready_or_set_waker(waker),
        }
    }
    fn val(&self) -> Self::Output {
        match &self.state {
            State::Barrier(_barrier, _reqs) => {
                unreachable!("should never be in barrier state when val is called");
            }
            State::Reqs(inner, _) => inner.val(),
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct ForEachAm<I, F>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    pub(crate) op: F,
    pub(crate) iter: ForEach<I, F>,
    pub(crate) schedule: IterSchedule,
}

impl<I, F> IterClone for ForEachAm<I, F>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    fn iter_clone(&self, _: Sealed) -> Self {
        ForEachAm {
            op: self.op.clone(),
            iter: self.iter.iter_clone(Sealed),
            schedule: self.schedule.clone(),
        }
    }
}

#[lamellar_impl::rt_am_local]
impl<I, F> LamellarAm for ForEachAm<I, F>
where
    I: DistributedIterator + 'static,
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
    I: DistributedIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    pub(crate) op: F,
    pub(crate) iter: ForEachAsync<I, F, Fut>,
    pub(crate) schedule: IterSchedule,
    // pub(crate) _phantom: PhantomData<Fut>
}

impl<I, F, Fut> IterClone for ForEachAsyncAm<I, F, Fut>
where
    I: DistributedIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn iter_clone(&self, _: Sealed) -> Self {
        ForEachAsyncAm {
            op: self.op.clone(),
            iter: self.iter.iter_clone(Sealed),
            schedule: self.schedule.clone(),
        }
    }
}

#[lamellar_impl::rt_am_local]
impl<I, F, Fut> LamellarAm for ForEachAsyncAm<I, F, Fut>
where
    I: DistributedIterator + 'static,
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
