use crate::active_messaging::LamellarArcLocalAm;
use crate::array::iterator::consumer::*;
use crate::array::iterator::distributed_iterator::DistributedIterator;
use crate::array::iterator::one_sided_iterator::OneSidedIterator;
use crate::array::iterator::IterRequest;
use crate::array::{ArrayOps, Distribution, LamellarArray, UnsafeArray};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::LamellarTeamRT;
use crate::Dist;

use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Sum<I> {
    pub(crate) iter: I,
}

impl<I> IterConsumer for Sum<I>
where
    I: DistributedIterator + 'static,
    I::Item: Dist + ArrayOps + std::iter::Sum,
{
    type AmOutput = I::Item;
    type Output = I::Item;
    type Item = I::Item;
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
            iter: self.clone(),
            schedule,
        })
    }
    fn create_handle(
        self,
        team: Pin<Arc<LamellarTeamRT>>,
        reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>,
    ) -> Box<dyn IterRequest<Output = Self::Output>> {
        Box::new(RemoteIterSumHandle { reqs, team })
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[doc(hidden)]
pub struct RemoteIterSumHandle<T> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = T>>>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
}

impl<T> RemoteIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    fn reduce_remote_vals(&self, local_sum: T, local_sums: UnsafeArray<T>) -> T {
        unsafe {
            local_sums.local_as_mut_slice()[0] = local_sum;
        };
        local_sums.barrier();
        let buffered_iter = unsafe { local_sums.buffered_onesided_iter(self.team.num_pes) };
        buffered_iter.into_iter().map(|&e| e).sum()
    }
}

#[doc(hidden)]
#[async_trait]
impl<T> IterRequest for RemoteIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.team.async_barrier().await;
        let local_sums = UnsafeArray::<T>::new(&self.team, self.team.num_pes, Distribution::Block);
        let local_sum = futures::future::join_all(self.reqs.drain(..).map(|req| req.into_future()))
            .await
            .into_iter()
            .sum();
        self.reduce_remote_vals(local_sum, local_sums)
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        let local_sums = UnsafeArray::<T>::new(&self.team, self.team.num_pes, Distribution::Block);
        let local_sum = self.reqs.drain(..).map(|req| req.get()).into_iter().sum();
        self.reduce_remote_vals(local_sum, local_sums)
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct SumAm<I> {
    pub(crate) iter: Sum<I>,
    pub(crate) schedule: IterSchedule,
}

#[lamellar_impl::rt_am_local]
impl<I> LamellarAm for SumAm<I>
where
    I: DistributedIterator + 'static,
    I::Item: Dist + ArrayOps + std::iter::Sum,
{
    async fn exec(&self) -> I::Item {
        let iter = self.schedule.init_iter(self.iter.clone());
        iter.sum::<I::Item>()
    }
}
