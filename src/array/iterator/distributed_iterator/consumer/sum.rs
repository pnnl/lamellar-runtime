use crate::active_messaging::LamellarArcLocalAm;
use crate::array::iterator::consumer::*;
use crate::array::iterator::distributed_iterator::DistributedIterator;
use crate::array::iterator::private::*;
use crate::array::{ArrayOps, Distribution, UnsafeArray};
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
pub struct Sum<I> {
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
    type Handle = DistIterSumHandle<I::Item>;
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
        DistIterSumHandle {
            reqs,
            team,
            state: State::ReqsPending(None),
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[doc(hidden)]
#[pin_project]
pub struct DistIterSumHandle<T> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<T>>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    state: State<T>,
}

enum State<T> {
    ReqsPending(Option<T>),
    Summing(Pin<Box<dyn Future<Output = T>>>),
}

impl<T> DistIterSumHandle<T>
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
        unsafe { local_sums.sum().await }
    }

    fn reduce_remote_vals(&self, local_sum: T, local_sums: UnsafeArray<T>) -> T {
        unsafe {
            local_sums.local_as_mut_slice()[0] = local_sum;
        };
        local_sums.tasking_barrier();
        // let buffered_iter = unsafe { local_sums.buffered_onesided_iter(self.team.num_pes) };
        // buffered_iter.into_iter().map(|&e| e).sum()
        unsafe { local_sums.sum().blocking_wait() }
    }
}

impl<T> Future for DistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            State::ReqsPending(local_sum) => {
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
                        *this.state = State::Summing(sum);
                        Poll::Pending
                    }
                }
            }
            State::Summing(sum) => {
                let local_sum = ready!(Future::poll(sum.as_mut(), cx));
                Poll::Ready(local_sum)
            }
        }
    }
}
#[doc(hidden)]
impl<T> LamellarRequest for DistIterSumHandle<T>
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
