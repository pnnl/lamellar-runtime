use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::distributed_iterator::DistributedIterator;
use crate::array::iterator::one_sided_iterator::OneSidedIterator;
use crate::array::iterator::{private::*, IterRequest};
use crate::array::{ArrayOps, Distribution, UnsafeArray};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::LamellarTeamRT;
use crate::Dist;

use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Reduce<I, F> {
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
    I: DistributedIterator + 'static,
    I::Item: Dist + ArrayOps,
    F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
{
    type AmOutput = Option<I::Item>;
    type Output = Option<I::Item>;
    type Item = I::Item;
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
        team: Pin<Arc<LamellarTeamRT>>,
        reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>,
    ) -> Box<dyn IterRequest<Output = Self::Output>> {
        Box::new(RemoteIterReduceHandle {
            op: self.op,
            reqs,
            team,
        })
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[doc(hidden)]
pub struct RemoteIterReduceHandle<T, F> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = Option<T>>>>,
    pub(crate) op: F,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
}

impl<T, F> RemoteIterReduceHandle<T, F>
where
    T: Dist + ArrayOps,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    async fn async_reduce_remote_vals(&self, local_val: Option<T>) -> Option<T> {
        self.team.async_barrier().await;
        let local_vals =
            UnsafeArray::<Option<T>>::new(&self.team, self.team.num_pes, Distribution::Block);
        unsafe {
            local_vals.local_as_mut_slice()[0] = local_val;
        };
        local_vals.async_barrier().await;
        let buffered_iter = unsafe { local_vals.buffered_onesided_iter(self.team.num_pes) };
        buffered_iter
            .into_iter()
            .filter_map(|&res| res)
            .reduce(self.op.clone())
    }

    fn reduce_remote_vals(&self, local_val: Option<T>) -> Option<T> {
        self.team.tasking_barrier();
        let local_vals =
            UnsafeArray::<Option<T>>::new(&self.team, self.team.num_pes, Distribution::Block);
        unsafe {
            local_vals.local_as_mut_slice()[0] = local_val;
        };
        local_vals.tasking_barrier();
        let buffered_iter = unsafe { local_vals.buffered_onesided_iter(self.team.num_pes) };
        buffered_iter
            .into_iter()
            .filter_map(|&res| res)
            .reduce(self.op.clone())
    }
}

#[doc(hidden)]
#[async_trait]
impl<T, F> IterRequest for RemoteIterReduceHandle<T, F>
where
    T: Dist + ArrayOps,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    type Output = Option<T>;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let local_val = futures::future::join_all(self.reqs.drain(..).map(|req| req.into_future()))
            .await
            .into_iter()
            .filter_map(|res| res)
            .reduce(self.op.clone());
        self.async_reduce_remote_vals(local_val).await
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        let local_val = self
            .reqs
            .drain(..)
            .filter_map(|req| req.get())
            .reduce(self.op.clone());
        self.reduce_remote_vals(local_val)
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
    I: DistributedIterator + 'static,
    I::Item: Dist + ArrayOps,
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
