use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::local_iterator::LocalIterator;
use crate::array::iterator::private::*;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::LamellarTask;
use crate::warnings::RuntimeWarning;

use futures_util::{ready, Future};
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

#[derive(Clone, Debug)]
pub(crate) struct Sum<I> {
    pub(crate) iter: I,
}

impl<I: IterClone> IterClone for Sum<I> {
    fn iter_clone(&self, _: Sealed) -> Self {
        Sum {
            iter: self.iter.iter_clone(Sealed),
        }
    }
}

impl<I> IterConsumer for Sum<I>
where
    I: LocalIterator + 'static,
    I::Item: SyncSend + for<'a> std::iter::Sum<&'a I::Item> + std::iter::Sum<I::Item>,
{
    type AmOutput = I::Item;
    type Output = I::Item;
    type Item = I::Item;
    type Handle = InnerLocalIterSumHandle<I::Item>;
    fn init(&self, start: usize, cnt: usize) -> Self {
        Sum {
            iter: self.iter.init(start, cnt),
        }
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(SumAm {
            iter: self.iter_clone(Sealed),
            schedule,
        })
    }
    fn create_handle(
        self,
        _team: Pin<Arc<LamellarTeamRT>>,
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        InnerLocalIterSumHandle {
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
pub(crate) struct InnerLocalIterSumHandle<T> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<T>>,
    state: InnerState<T>,
}

enum InnerState<T> {
    ReqsPending(Option<T>),
}

impl<T> Future for InnerLocalIterSumHandle<T>
where
    T: SyncSend + for<'a> std::iter::Sum<&'a T> + std::iter::Sum<T> + 'static,
{
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            InnerState::ReqsPending(local_sum) => {
                while let Some(mut req) = this.reqs.pop_front() {
                    if !req.ready_or_set_waker(cx.waker()) {
                        this.reqs.push_front(req);
                        return Poll::Pending;
                    }
                    match local_sum {
                        Some(sum) => {
                            *sum = [sum, &req.val()].into_iter().sum::<T>();
                        }
                        None => {
                            *local_sum = Some(req.val());
                        }
                    }
                }

                Poll::Ready(local_sum.take().expect("Value should be Present"))
            }
        }
    }
}

//#[doc(hidden)]
impl<T> LamellarRequest for InnerLocalIterSumHandle<T>
where
    T: SyncSend + for<'a> std::iter::Sum<&'a T> + std::iter::Sum<T> + 'static,
{
    fn blocking_wait(mut self) -> Self::Output {
        self.reqs
            .drain(..)
            .map(|req| req.blocking_wait())
            .sum::<Self::Output>()
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
        self.reqs.iter().map(|req| req.val()).sum::<Self::Output>()
    }
}

#[pin_project]
pub struct LocalIterSumHandle<T> {
    team: Pin<Arc<LamellarTeamRT>>,
    #[pin]
    state: State<T>,
}

impl<T> LocalIterSumHandle<T>
where
    T: SyncSend + for<'a> std::iter::Sum<&'a T> + std::iter::Sum<T> + 'static,
{
    pub(crate) fn new(
        inner: Pin<Box<dyn Future<Output = InnerLocalIterSumHandle<T>> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        Self {
            team: array.data.team.clone(),
            state: State::Init(inner),
        }
    }

    /// This method will block until the associated Sumoperation completes and returns the result
    pub fn block(self) -> T {
        RuntimeWarning::BlockingCall(
            "LocalIterSumHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }
    /// This method will spawn the associated Sum Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<T> {
        self.team.clone().scheduler.spawn_task(self)
    }
}

#[pin_project(project = StateProj)]
enum State<T> {
    Init(Pin<Box<dyn Future<Output = InnerLocalIterSumHandle<T>> + Send>>),
    Reqs(#[pin] InnerLocalIterSumHandle<T>),
}
impl<T> Future for LocalIterSumHandle<T>
where
    T: SyncSend + for<'a> std::iter::Sum<&'a T> + std::iter::Sum<T> + 'static,
{
    type Output = T;
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
impl<T> LamellarRequest for LocalIterSumHandle<T>
where
    T: SyncSend + for<'a> std::iter::Sum<&'a T> + std::iter::Sum<T> + 'static,
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
pub(crate) struct SumAm<I> {
    pub(crate) iter: Sum<I>,
    pub(crate) schedule: IterSchedule,
}

impl<I: IterClone> IterClone for SumAm<I> {
    fn iter_clone(&self, _: Sealed) -> Self {
        SumAm {
            iter: self.iter.iter_clone(Sealed),
            schedule: self.schedule.clone(),
        }
    }
}

#[lamellar_impl::rt_am_local]
impl<I> LamellarAm for SumAm<I>
where
    I: LocalIterator + 'static,
    I::Item: SyncSend + for<'a> std::iter::Sum<&'a I::Item> + std::iter::Sum<I::Item>,
{
    async fn exec(&self) -> I::Item {
        let iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        iter.sum::<I::Item>()
    }
}
