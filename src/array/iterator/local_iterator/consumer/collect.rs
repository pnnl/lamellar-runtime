use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::local_iterator::{LocalIterator, Monotonic};
use crate::array::iterator::IterRequest;
use crate::array::operations::ArrayOps;
use crate::array::{AsyncTeamFrom, AsyncTeamInto, Distribution, TeamInto};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::LamellarTeamRT;
use crate::memregion::Dist;

use async_trait::async_trait;
use core::marker::PhantomData;
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
    I: LocalIterator,
    I::Item: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
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
        Box::new(LocalIterCollectHandle {
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

// impl<I,A> MonotonicIterConsumer for Collect<I,A>
// where
//     I: LocalIterator,
//     I::Item: Dist + ArrayOps,
//     A: for<'a> TeamFrom<(&'a Vec<I::Item>,Distribution)> + SyncSend  + Clone +  'static,{
//     fn monotonic<J: IterConsumer>(&self) -> J {
//         Collect{
//             iter: self.iter.monotonic(),
//             distribution: self.distribution.clone(),
//             _phantom: self._phantom.clone(),
//         }
//     }
// }

// #[derive(Clone,Debug)]
// pub struct CollectAsync<I,A,B>{
//     pub(crate) iter: I,
//     pub(crate) distribution: Distribution,
//     pub(crate) _phantom: PhantomData<(A,B)>
// }

// impl<I,A,B> IterConsumer for CollectAsync<I,A,B>
// where
//     I: LocalIterator,
//    I::Item: Future<Output = B> + Send  + 'static,
//     B: Dist + ArrayOps,
//     A: From<UnsafeArray<B>> + SyncSend + Clone + 'static,{
//     type AmOutput = Vec<(usize,B)>;
//     type Output = A;
//     fn into_am(self, schedule: IterSchedule) -> LamellarArcLocalAm {
//         Arc::new(CollectAsyncAm{
//             iter: self.iter,
//             schedule
//         })
//     }
//     fn create_handle(self, team: Pin<Arc<LamellarTeamRT>>, reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>) -> Box<dyn IterRequest<Output = Self::Output>> {
//         Box::new(LocalIterCollectHandle {
//             reqs,
//             distribution: self.distribution,
//             team,
//             _phantom: PhantomData,
//         })
//     }
//     fn max_elems(&self, in_elems: usize) -> usize{
//         self.iter.elems(in_elems)
//     }
// }

#[doc(hidden)]
pub struct LocalIterCollectHandle<
    T: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend,
> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = Vec<(usize, T)>>>>,
    pub(crate) distribution: Distribution,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) _phantom: PhantomData<A>,
}

impl<T: Dist + ArrayOps, A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend>
    LocalIterCollectHandle<T, A>
{
    async fn async_create_array(&self, local_vals: Vec<T>) -> A {
        let input = (local_vals, self.distribution);
        AsyncTeamInto::team_into(input, &self.team).await
    }
    fn create_array(&self, local_vals: Vec<T>) -> A {
        let input = (local_vals, self.distribution);
        TeamInto::team_into(input, &self.team)
    }
}
#[async_trait]
impl<T: Dist + ArrayOps, A: AsyncTeamFrom<(Vec<T>, Distribution)> + SyncSend> IterRequest
    for LocalIterCollectHandle<T, A>
{
    type Output = A;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let mut temp_vals = vec![];
        for req in self.reqs.drain(0..) {
            let v = req.into_future().await;
            temp_vals.extend(v);
        }
        temp_vals.sort_by(|a, b| a.0.cmp(&b.0));
        let local_vals = temp_vals.into_iter().map(|v| v.1).collect();
        self.async_create_array(local_vals).await
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
        self.create_array(local_vals)
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
    I: LocalIterator,
    I::Item: Dist + ArrayOps,
    A: AsyncTeamFrom<(Vec<I::Item>, Distribution)> + SyncSend + Clone + 'static,
{
    async fn exec(&self) -> Vec<I::Item> {
        let iter = self.schedule.init_iter(self.iter.clone());
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
//         let mut iter = self.schedule.monotonic_iter(self.iter.clone());
//         let mut res = vec![];
//         while let Some(elem) = iter.next(){
//             res.push(elem.await);
//         }
//         res
//     }
// }
