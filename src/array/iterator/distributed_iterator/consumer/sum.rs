use crate::active_messaging::LamellarArcLocalAm;
use crate::array::iterator::consumer::*;
use crate::array::iterator::distributed_iterator::DistributedIterator;
use crate::array::iterator::private::*;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::{ArrayOps, Distribution, UnsafeArray};
use crate::barrier::BarrierHandle;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;
use crate::Dist;
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
    I: DistributedIterator + 'static,
    I::Item: Dist + ArrayOps + std::iter::Sum,
{
    type AmOutput = I::Item;
    type Output = I::Item;
    type Item = I::Item;
    type Handle = InnerDistIterSumHandle<I::Item>;
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
        team: Pin<Arc<LamellarTeamRT>>,
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        InnerDistIterSumHandle {
            reqs,
            team,
            state: InnerState::ReqsPending(None),
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

//#[doc(hidden)]
#[pin_project]
pub(crate) struct InnerDistIterSumHandle<T> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<T>>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    state: InnerState<T>,
}

enum InnerState<T> {
    ReqsPending(Option<T>),
    Summing(Pin<Box<dyn Future<Output = T> + Send>>),
}

impl<T> InnerDistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    async fn async_reduce_remote_vals(local_sum: T, team: Pin<Arc<LamellarTeamRT>>) -> T {
        let local_sums =
            UnsafeArray::<T>::async_new(&team, team.num_pes, Distribution::Block).await;
        unsafe {
            local_sums.local_as_mut_slice()[0] = local_sum;
        };
        local_sums.async_barrier().await;
        // let buffered_iter = unsafe { local_sums.buffered_onesided_iter(self.team.num_pes) };
        // buffered_iter.into_iter().map(|&e| e).sum()
        unsafe {
            local_sums
                .sum()
                .await
                .expect("array size is greater than zero")
        }
    }

    fn reduce_remote_vals(&self, local_sum: T, local_sums: UnsafeArray<T>) -> T {
        unsafe {
            local_sums.local_as_mut_slice()[0] = local_sum;
        };
        local_sums.tasking_barrier();
        // let buffered_iter = unsafe { local_sums.buffered_onesided_iter(self.team.num_pes) };
        // buffered_iter.into_iter().map(|&e| e).sum()
        unsafe {
            local_sums
                .sum()
                .blocking_wait()
                .expect("array size is greater than zero")
        }
    }
}

impl<T> Future for InnerDistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
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
                            *sum = [*sum, req.val()].into_iter().sum();
                        }
                        None => {
                            *local_sum = Some(req.val());
                        }
                    }
                }
                let mut sum = Box::pin(Self::async_reduce_remote_vals(
                    local_sum.unwrap(),
                    this.team.clone(),
                ));
                match Future::poll(sum.as_mut(), cx) {
                    Poll::Ready(local_sum) => Poll::Ready(local_sum),
                    Poll::Pending => {
                        *this.state = InnerState::Summing(sum);
                        Poll::Pending
                    }
                }
            }
            InnerState::Summing(sum) => {
                let local_sum = ready!(Future::poll(sum.as_mut(), cx));
                Poll::Ready(local_sum)
            }
        }
    }
}
//#[doc(hidden)]
impl<T> LamellarRequest for InnerDistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    fn blocking_wait(mut self) -> Self::Output {
        let local_sums = UnsafeArray::<T>::new(&self.team, self.team.num_pes, Distribution::Block);
        let local_sum = self
            .reqs
            .drain(..)
            .map(|req| req.blocking_wait())
            .into_iter()
            .sum();
        self.reduce_remote_vals(local_sum, local_sums)
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
        let local_sums = UnsafeArray::<T>::new(&self.team, self.team.num_pes, Distribution::Block);
        let local_sum = self.reqs.iter().map(|req| req.val()).into_iter().sum();
        self.reduce_remote_vals(local_sum, local_sums)
    }
}

#[pin_project]
pub struct DistIterSumHandle<T> {
    team: Pin<Arc<LamellarTeamRT>>,
    #[pin]
    state: State<T>,
}

impl<T> DistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    pub(crate) fn new(
        barrier_handle: BarrierHandle,
        inner: Pin<Box<dyn Future<Output = InnerDistIterSumHandle<T>> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        Self {
            team: array.data.team.clone(),
            state: State::Barrier(barrier_handle, inner),
        }
    }
}

#[pin_project(project = StateProj)]
enum State<T> {
    Barrier(
        #[pin] BarrierHandle,
        Pin<Box<dyn Future<Output = InnerDistIterSumHandle<T>> + Send>>,
    ),
    Reqs(#[pin] InnerDistIterSumHandle<T>),
}
impl<T> Future for DistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match this.state.as_mut().project() {
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
        }
    }
}

//#[doc(hidden)]
impl<T> LamellarRequest for DistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    fn blocking_wait(self) -> Self::Output {
        match self.state {
            State::Barrier(barrier, reqs) => {
                barrier.blocking_wait();
                self.team.block_on(reqs).blocking_wait()
            }
            State::Reqs(inner) => inner.blocking_wait(),
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
            State::Reqs(inner) => inner.ready_or_set_waker(waker),
        }
    }
    fn val(&self) -> Self::Output {
        match &self.state {
            State::Barrier(_barrier, _reqs) => {
                unreachable!("should never be in barrier state when val is called");
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
    I: DistributedIterator + 'static,
    I::Item: Dist + ArrayOps + std::iter::Sum,
{
    async fn exec(&self) -> I::Item {
        let iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        iter.sum::<I::Item>()
    }
}
