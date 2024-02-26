use crate::active_messaging::LamellarArcLocalAm;
use crate::array::iterator::consumer::*;
use crate::array::iterator::distributed_iterator::DistributedIterator;
use crate::array::iterator::{private::*, IterRequest};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::SchedulerQueue;
use crate::Darc;

use async_trait::async_trait;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone, Debug)]
pub struct Count<I> {
    pub(crate) iter: I,
}

impl<I: IterClone> IterClone for Count<I> {
    fn iter_clone(&self, _: Sealed) -> Self {
        Count {
            iter: self.iter.iter_clone(Sealed),
        }
    }
}

impl<I> IterConsumer for Count<I>
where
    I: DistributedIterator,
{
    type AmOutput = usize;
    type Output = usize;
    type Item = I::Item;
    fn init(&self, start: usize, cnt: usize) -> Self {
        Count {
            iter: self.iter.init(start, cnt),
        }
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(CountAm {
            iter: self.iter_clone(Sealed),
            schedule,
        })
    }
    fn create_handle(
        self,
        team: Pin<Arc<LamellarTeamRT>>,
        reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>,
    ) -> Box<dyn IterRequest<Output = Self::Output>> {
        Box::new(RemoteIterCountHandle { reqs, team })
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[doc(hidden)]
pub struct RemoteIterCountHandle {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = usize>>>,
    team: Pin<Arc<LamellarTeamRT>>,
}

#[lamellar_impl::AmDataRT]
struct UpdateCntAm {
    remote_cnt: usize,
    cnt: Darc<AtomicUsize>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for UpdateCntAm {
    async fn exec(self) {
        self.cnt.fetch_add(self.remote_cnt, Ordering::Relaxed);
    }
}

impl RemoteIterCountHandle {
    async fn reduce_remote_counts(&self, local_cnt: usize, cnt: Darc<AtomicUsize>) -> usize {
        self.team
            .exec_am_all(UpdateCntAm {
                remote_cnt: local_cnt,
                cnt: cnt.clone(),
            })
            .into_future()
            .await;
        self.team.async_barrier().await;
        cnt.load(Ordering::SeqCst)
    }
}

#[doc(hidden)]
#[async_trait]
impl IterRequest for RemoteIterCountHandle {
    type Output = usize;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.team.async_barrier().await;
        let cnt = Darc::new(&self.team, AtomicUsize::new(0)).unwrap();
        // all the requests should have already been launched, and we are just awaiting the results
        let count = futures::future::join_all(self.reqs.drain(..).map(|req| req.into_future()))
            .await
            .into_iter()
            .sum::<usize>();
        // println!("count: {} {:?}", count, std::thread::current().id());
        self.reduce_remote_counts(count, cnt).await
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        self.team.tasking_barrier();
        let cnt = Darc::new(&self.team, AtomicUsize::new(0)).unwrap();
        let count = self
            .reqs
            .drain(..)
            .map(|req| req.get())
            .into_iter()
            .sum::<usize>();
        self.team
            .scheduler
            .block_on(self.reduce_remote_counts(count, cnt))
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct CountAm<I> {
    pub(crate) iter: Count<I>,
    pub(crate) schedule: IterSchedule,
}

impl<I> IterClone for CountAm<I>
where
    I: IterClone,
{
    fn iter_clone(&self, _: Sealed) -> Self {
        CountAm {
            iter: self.iter.iter_clone(Sealed),
            schedule: self.schedule.clone(),
        }
    }
}

#[lamellar_impl::rt_am_local]
impl<I> LamellarAm for CountAm<I>
where
    I: DistributedIterator + 'static,
{
    async fn exec(&self) -> usize {
        let mut iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        let mut count: usize = 0;
        while let Some(_) = iter.next() {
            count += 1;
        }
        // println!("count: {} {:?}", count, std::thread::current().id());
        count
    }
}
