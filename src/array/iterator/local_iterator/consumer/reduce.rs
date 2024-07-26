use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::local_iterator::LocalIterator;
use crate::array::iterator::private::*;
use crate::array::r#unsafe::private::UnsafeArrayInner;
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
pub(crate) struct Reduce<I, F> {
    pub(crate) iter: I,
    pub(crate) op: F,
}

impl<I: IterClone, F: Clone> IterClone for Reduce<I, F> {
    fn iter_clone(&self, _: Sealed) -> Self {
        Reduce {
            iter: self.iter.iter_clone(Sealed),
            op: self.op.clone(),
        }
    }
}

impl<I, F> IterConsumer for Reduce<I, F>
where
    I: LocalIterator + 'static,
    I::Item: SyncSend + Copy,
    F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
{
    type AmOutput = Option<I::Item>;
    type Output = Option<I::Item>;
    type Item = I::Item;
    type Handle = InnerLocalIterReduceHandle<I::Item, F>;
    fn init(&self, start: usize, cnt: usize) -> Self {
        Reduce {
            iter: self.iter.init(start, cnt),
            op: self.op.clone(),
        }
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(ReduceAm {
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
        InnerLocalIterReduceHandle {
            op: self.op,
            reqs,
            state: InnerState::ReqsPending(None),
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

//#[doc(hidden)]
#[pin_project]
pub(crate) struct InnerLocalIterReduceHandle<T, F> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<Option<T>>>,
    pub(crate) op: F,
    state: InnerState<T>,
}

enum InnerState<T> {
    ReqsPending(Option<T>),
}

impl<T, F> Future for InnerLocalIterReduceHandle<T, F>
where
    T: SyncSend + Copy + 'static,
    F: Fn(T, T) -> T + SyncSend + 'static,
{
    type Output = Option<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            InnerState::ReqsPending(val) => {
                while let Some(mut req) = this.reqs.pop_front() {
                    if !req.ready_or_set_waker(cx.waker()) {
                        this.reqs.push_front(req);
                        return Poll::Pending;
                    }
                    match val {
                        None => *val = req.val(),
                        Some(val1) => {
                            if let Some(val2) = req.val() {
                                *val = Some((this.op)(*val1, val2));
                            }
                        }
                    }
                }
                Poll::Ready(*val)
            }
        }
    }
}

//#[doc(hidden)]
impl<T, F> LamellarRequest for InnerLocalIterReduceHandle<T, F>
where
    T: SyncSend + Copy + 'static,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    fn blocking_wait(mut self) -> Self::Output {
        self.reqs
            .drain(..)
            .filter_map(|req| req.blocking_wait())
            .reduce(self.op)
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
        self.reqs
            .iter()
            .filter_map(|req| req.val())
            .reduce(self.op.clone())
    }
}

#[pin_project]
pub struct LocalIterReduceHandle<T, F> {
    team: Pin<Arc<LamellarTeamRT>>,
    #[pin]
    state: State<T, F>,
}

impl<T, F> LocalIterReduceHandle<T, F>
where
    T: SyncSend + Copy + 'static,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    pub(crate) fn new(
        reqs: Pin<Box<dyn Future<Output = InnerLocalIterReduceHandle<T, F>> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        Self {
            team: array.data.team.clone(),
            state: State::Init(reqs),
        }
    }

    /// This method will block until the associated Reduce operation completes and returns the result
    pub fn block(self) -> Option<T> {
        self.team.clone().block_on(self)
    }

    /// This method will spawn the associated Reduce Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]

    pub fn spawn(self) -> LamellarTask<Option<T>> {
        self.team.clone().scheduler.spawn_task(self)
    }
}

#[pin_project(project = StateProj)]
enum State<T, F> {
    Init(Pin<Box<dyn Future<Output = InnerLocalIterReduceHandle<T, F>> + Send>>),
    Reqs(#[pin] InnerLocalIterReduceHandle<T, F>),
}
impl<T, F> Future for LocalIterReduceHandle<T, F>
where
    T: SyncSend + Copy + 'static,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    type Output = Option<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match this.state.as_mut().project() {
            StateProj::Init(inner) => {
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
        }
    }
}

//#[doc(hidden)]
impl<T, F> LamellarRequest for LocalIterReduceHandle<T, F>
where
    T: SyncSend + Copy + 'static,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    fn blocking_wait(self) -> Self::Output {
        match self.state {
            State::Init(reqs) => self.team.block_on(reqs).blocking_wait(),
            State::Reqs(inner) => inner.blocking_wait(),
        }
    }
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
        match &mut self.state {
            State::Init(_) => {
                waker.wake_by_ref();
                false
            }
            State::Reqs(inner) => inner.ready_or_set_waker(waker),
        }
    }
    fn val(&self) -> Self::Output {
        match &self.state {
            State::Init(_reqs) => {
                unreachable!("should never be in init state when val is called");
            }
            State::Reqs(inner) => inner.val(),
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct ReduceAm<I, F> {
    pub(crate) op: F,
    pub(crate) iter: Reduce<I, F>,
    pub(crate) schedule: IterSchedule,
}

impl<I: IterClone, F: Clone> IterClone for ReduceAm<I, F> {
    fn iter_clone(&self, _: Sealed) -> Self {
        ReduceAm {
            op: self.op.clone(),
            iter: self.iter.iter_clone(Sealed),
            schedule: self.schedule.clone(),
        }
    }
}

#[lamellar_impl::rt_am_local]
impl<I, F> LamellarAm for ReduceAm<I, F>
where
    I: LocalIterator + 'static,
    I::Item: SyncSend + Copy,
    F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
{
    async fn exec(&self) -> Option<I::Item> {
        let mut iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        match iter.next() {
            Some(mut accum) => {
                while let Some(elem) = iter.next() {
                    accum = (self.op)(accum, elem);
                }
                Some(accum)
            }
            None => None,
        }
    }
}
