use crate::active_messaging::LamellarArcLocalAm;
use crate::array::iterator::consumer::*;
use crate::array::iterator::distributed_iterator::DistributedIterator;
use crate::array::iterator::private::*;

use crate::darc::DarcMode;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;
use crate::Darc;

use async_trait::async_trait;
use futures_util::{ready, Future};
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll, Waker};

#[derive(Clone, Debug)]
pub(crate) struct Count<I> {
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
    type Handle = DistIterCountHandle;
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
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        DistIterCountHandle {
            reqs,
            team,
            state: State::ReqsPending(0),
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

//#[doc(hidden)]
#[pin_project]
pub struct DistIterCountHandle {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<usize>>,
    team: Pin<Arc<LamellarTeamRT>>,
    state: State,
}

enum State {
    ReqsPending(usize),
    Counting(Pin<Box<dyn Future<Output = usize>>>),
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

impl DistIterCountHandle {
    async fn async_reduce_remote_counts(local_cnt: usize, team: Pin<Arc<LamellarTeamRT>>) -> usize {
        let cnt = Darc::async_try_new(&team, AtomicUsize::new(0), DarcMode::Darc)
            .await
            .unwrap();
        team.exec_am_all(UpdateCntAm {
            remote_cnt: local_cnt,
            cnt: cnt.clone(),
        })
        .await;
        team.async_barrier().await;
        cnt.load(Ordering::SeqCst)
    }

    fn reduce_remote_counts(&self, local_cnt: usize, cnt: Darc<AtomicUsize>) -> usize {
        self.team.exec_am_all(UpdateCntAm {
            remote_cnt: local_cnt,
            cnt: cnt.clone(),
        });
        self.team.wait_all();
        self.team.tasking_barrier();
        cnt.load(Ordering::SeqCst)
    }
}

impl Future for DistIterCountHandle {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            State::ReqsPending(cnt) => {
                while let Some(mut req) = this.reqs.pop_front() {
                    if !req.ready_or_set_waker(cx.waker()) {
                        this.reqs.push_front(req);
                        return Poll::Pending;
                    }
                    *cnt += req.val();
                }
                let mut global_cnt =
                    Box::pin(Self::async_reduce_remote_counts(*cnt, this.team.clone()));
                match Future::poll(global_cnt.as_mut(), cx) {
                    Poll::Ready(count) => {
                        return Poll::Ready(count);
                    }
                    Poll::Pending => {
                        *this.state = State::Counting(global_cnt);
                        Poll::Pending
                    }
                }
            }
            State::Counting(global_cnt) => {
                let count = ready!(Future::poll(global_cnt.as_mut(), cx));
                Poll::Ready(count)
            }
        }
    }
}

//#[doc(hidden)]
#[async_trait]
impl LamellarRequest for DistIterCountHandle {
    fn blocking_wait(mut self) -> Self::Output {
        self.team.tasking_barrier();
        let cnt = Darc::new(&self.team, AtomicUsize::new(0)).unwrap();
        let count = self
            .reqs
            .drain(..)
            .map(|req| req.blocking_wait())
            .into_iter()
            .sum::<usize>();
        self.reduce_remote_counts(count, cnt)
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
        self.team.tasking_barrier();
        let cnt = Darc::new(&self.team, AtomicUsize::new(0)).unwrap();
        let count = self
            .reqs
            .iter()
            .map(|req| req.val())
            .into_iter()
            .sum::<usize>();
        self.reduce_remote_counts(count, cnt)
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
