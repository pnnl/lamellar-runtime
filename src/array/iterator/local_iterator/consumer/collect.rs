use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::local_iterator::{LocalIterator, Monotonic};
use crate::array::iterator::private::*;
use crate::array::operations::ArrayOps;
use crate::array::{AsyncTeamFrom, AsyncTeamInto, Distribution, TeamInto};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;
use crate::memregion::Dist;

use core::marker::PhantomData;
use futures_util::{ready, Future};
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

#[derive(Clone, Debug)]
pub struct Collect<I, A> {
    pub(crate) iter: Monotonic<I>,
    pub(crate) distribution: Distribution,
    pub(crate) _phantom: PhantomData<A>,
}

impl<I: IterClone, A> IterClone for Collect<I, A> {
    fn iter_clone(&self, _: Sealed) -> Self {
        Collect {
            iter: self.iter.iter_clone(Sealed),
            distribution: self.distribution.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

impl<I, A> IterConsumer for Collect<I, A>
where
    I: LocalIterator,
    I::Item: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
{
    type AmOutput = Vec<(usize, I::Item)>;
    type Output = A;
    type Item = (usize, I::Item);
    type Handle = LocalIterCollectHandle<I::Item, A>;
    fn init(&self, start: usize, cnt: usize) -> Self {
        Collect {
            iter: self.iter.init(start, cnt),
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
        LocalIterCollectHandle {
            reqs,
            distribution: self.distribution,
            team,
            state: State::ReqsPending(Vec::new()),
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[doc(hidden)]
#[pin_project]
pub struct LocalIterCollectHandle<
    T: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend,
> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<Vec<(usize, T)>>>,
    pub(crate) distribution: Distribution,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    state: State<T, A>,
}

enum State<T: Dist + ArrayOps, A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend> {
    ReqsPending(Vec<(usize, T)>),
    Collecting(Pin<Box<dyn Future<Output = A>>>),
}

impl<T: Dist + ArrayOps, A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + 'static>
    LocalIterCollectHandle<T, A>
{
    async fn async_create_array(
        local_vals: Vec<T>,
        dist: Distribution,
        team: Pin<Arc<LamellarTeamRT>>,
    ) -> A {
        let input = (local_vals, dist);
        AsyncTeamInto::team_into(input, &team).await
    }
    fn create_array(&self, local_vals: Vec<T>) -> A {
        let input = (local_vals, self.distribution);
        TeamInto::team_into(input, &self.team)
    }
}

impl<T: Dist + ArrayOps, A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + 'static> Future
    for LocalIterCollectHandle<T, A>
{
    type Output = A;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            State::ReqsPending(vals) => {
                while let Some(mut req) = this.reqs.pop_front() {
                    if !req.ready_or_set_waker(cx.waker()) {
                        this.reqs.push_front(req);
                        return Poll::Pending;
                    } else {
                        vals.extend(req.val());
                    }
                }
                vals.sort_by(|a, b| a.0.cmp(&b.0));
                let local_vals = vals.into_iter().map(|v| v.1).collect();
                let mut collect = Box::pin(Self::async_create_array(
                    local_vals,
                    this.distribution.clone(),
                    this.team.clone(),
                ));
                match Future::poll(collect.as_mut(), cx) {
                    Poll::Ready(a) => {
                        return Poll::Ready(a);
                    }
                    Poll::Pending => {
                        *this.state = State::Collecting(collect);
                        return Poll::Pending;
                    }
                }
            }
            State::Collecting(collect) => {
                let a = ready!(Future::poll(collect.as_mut(), cx));
                return Poll::Ready(a);
            }
        }
    }
}

impl<T: Dist + ArrayOps, A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend + 'static>
    LamellarRequest for LocalIterCollectHandle<T, A>
{
    fn blocking_wait(mut self) -> Self::Output {
        // let mut num_local_vals = 0;
        let mut temp_vals = vec![];
        for req in self.reqs.drain(0..) {
            let v = req.blocking_wait();
            temp_vals.extend(v);
        }
        temp_vals.sort_by(|a, b| a.0.cmp(&b.0));
        let local_vals = temp_vals.into_iter().map(|v| v.1).collect();
        self.create_array(local_vals)
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
        // let mut num_local_vals = 0;
        let mut temp_vals = vec![];
        for req in self.reqs.iter() {
            let v = req.val();
            temp_vals.extend(v);
        }
        temp_vals.sort_by(|a, b| a.0.cmp(&b.0));
        let local_vals = temp_vals.into_iter().map(|v| v.1).collect();
        self.create_array(local_vals)
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct CollectAm<I, A> {
    pub(crate) iter: Collect<I, A>,
    pub(crate) schedule: IterSchedule,
}

impl<I: IterClone, A> IterClone for CollectAm<I, A> {
    fn iter_clone(&self, _: Sealed) -> Self {
        CollectAm {
            iter: self.iter.iter_clone(Sealed),
            schedule: self.schedule.clone(),
        }
    }
}

#[lamellar_impl::rt_am_local]
impl<I, A> LamellarAm for CollectAm<I, A>
where
    I: LocalIterator,
    I::Item: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
{
    async fn exec(&self) -> Vec<I::Item> {
        let iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        iter.collect::<Vec<_>>()
    }
}

// #[lamellar_impl::AmLocalDataRT(Clone)]
// pub(crate) struct CollectAsyncAm<I>
// where
//     I: LocalIterator,
// {
//     pub(crate) iter: I,
//     pub(crate) schedule: IterSchedule,
// }

// #[lamellar_impl::rt_am_local]
// impl<I> LamellarAm for CollectAsyncAm<I>
// where
//     I: LocalIterator + 'static,
//     I::Item: Sync,
// {
//     async fn exec(&self) -> Vec<(usize,I::Item)> {
//         let mut iter = self.schedule.monotonic_iter(self.iter.iter_clone(Sealed));
//         let mut res = vec![];
//         while let Some(elem) = iter.next(){
//             res.push(elem.await);
//         }
//         res
//     }
// }
