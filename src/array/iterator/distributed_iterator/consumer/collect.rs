use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::distributed_iterator::{DistributedIterator, Monotonic};
use crate::array::iterator::IterRequest;
use crate::array::operations::ArrayOps;
use crate::array::{Distribution, TeamFrom, TeamInto};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::LamellarTeamRT;
use crate::memregion::Dist;

use async_trait::async_trait;
use core::marker::PhantomData;
use futures::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Collect<I, A> {
    pub(crate) iter: Monotonic<I>,
    pub(crate) distribution: Distribution,
    pub(crate) _phantom: PhantomData<A>,
}

impl<I, A> IterConsumer for Collect<I, A>
where
    I: DistributedIterator,
    I::Item: Dist + ArrayOps,
    A: for<'a> TeamFrom<(&'a Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
{
    type AmOutput = Vec<(usize, I::Item)>;
    type Output = A;
    type Item = (usize, I::Item);
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
            iter: self.clone(),
            schedule,
        })
    }
    fn create_handle(
        self,
        team: Pin<Arc<LamellarTeamRT>>,
        reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>,
    ) -> Box<dyn IterRequest<Output = Self::Output>> {
        Box::new(DistIterCollectHandle {
            reqs,
            distribution: self.distribution,
            team,
            _phantom: self._phantom,
        })
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[derive(Debug)]
pub struct CollectAsync<I, A, B> {
    pub(crate) iter: Monotonic<I>,
    pub(crate) distribution: Distribution,
    pub(crate) _phantom: PhantomData<(A, B)>,
}

impl<I, A, B> IterConsumer for CollectAsync<I, A, B>
where
    I: DistributedIterator,
    I::Item: Future<Output = B> + Send + 'static,
    B: Dist + ArrayOps,
    A: for<'a> TeamFrom<(&'a Vec<B>, Distribution)> + SyncSend + Clone + 'static,
{
    type AmOutput = Vec<(usize, B)>;
    type Output = A;
    type Item = (usize, I::Item);
    fn init(&self, start: usize, cnt: usize) -> Self {
        CollectAsync {
            iter: self.iter.init(start, cnt),
            distribution: self.distribution.clone(),
            _phantom: self._phantom.clone(),
        }
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(CollectAsyncAm {
            iter: self.clone(),
            schedule,
        })
    }
    fn create_handle(
        self,
        team: Pin<Arc<LamellarTeamRT>>,
        reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>,
    ) -> Box<dyn IterRequest<Output = Self::Output>> {
        Box::new(DistIterCollectHandle {
            reqs,
            distribution: self.distribution,
            team,
            _phantom: PhantomData,
        })
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

impl<I, A, B> Clone for CollectAsync<I, A, B>
where
    I: DistributedIterator,
    I::Item: Future<Output = B> + Send + 'static,
    B: Dist + ArrayOps,
    A: for<'a> TeamFrom<(&'a Vec<B>, Distribution)> + SyncSend + Clone + 'static,
{
    fn clone(&self) -> Self {
        CollectAsync {
            iter: self.iter.clone(),
            distribution: self.distribution.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

#[doc(hidden)]
pub struct DistIterCollectHandle<
    T: Dist + ArrayOps,
    A: for<'a> TeamFrom<(&'a Vec<T>, Distribution)> + SyncSend,
> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = Vec<(usize, T)>>>>,
    pub(crate) distribution: Distribution,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) _phantom: PhantomData<A>,
}

impl<T: Dist + ArrayOps, A: for<'a> TeamFrom<(&'a Vec<T>, Distribution)> + SyncSend>
    DistIterCollectHandle<T, A>
{
    fn create_array(&self, local_vals: &Vec<T>) -> A {
        let input = (local_vals, self.distribution);
        input.team_into(&self.team)
    }
}
#[async_trait]
impl<T: Dist + ArrayOps, A: for<'a> TeamFrom<(&'a Vec<T>, Distribution)> + SyncSend> IterRequest
    for DistIterCollectHandle<T, A>
{
    type Output = A;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let mut temp_vals = vec![];
        for req in self.reqs.drain(0..) {
            let v = req.into_future().await;
            temp_vals.extend(v);
        }
        temp_vals.sort_by(|a, b| a.0.cmp(&b.0));
        let local_vals = temp_vals.into_iter().map(|v| v.1).collect::<Vec<_>>();
        self.create_array(&local_vals)
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        // let mut num_local_vals = 0;
        let mut temp_vals = vec![];
        for req in self.reqs.drain(0..) {
            let v = req.get();
            temp_vals.extend(v);
        }
        temp_vals.sort_by(|a, b| a.0.cmp(&b.0));
        let local_vals = temp_vals.into_iter().map(|v| v.1).collect();
        self.create_array(&local_vals)
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
    A: for<'a> TeamFrom<(&'a Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
{
    async fn exec(&self) -> Vec<I::Item> {
        let iter = self.schedule.init_iter(self.iter.clone());
        iter.collect::<Vec<_>>()
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct CollectAsyncAm<I, A, B>
where
    I: DistributedIterator,
    I::Item: Future<Output = B> + Send + 'static,
    B: Dist + ArrayOps,
    A: for<'a> TeamFrom<(&'a Vec<B>, Distribution)> + SyncSend + Clone + 'static,
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
    A: for<'a> TeamFrom<(&'a Vec<B>, Distribution)> + SyncSend + Clone + 'static,
{
    async fn exec(&self) -> Vec<(usize, B)> {
        let mut iter = self.schedule.init_iter(self.iter.clone());
        let mut res = vec![];
        while let Some((index, elem)) = iter.next() {
            res.push((index, elem.await));
        }
        res
    }
}
