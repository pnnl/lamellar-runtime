use crate::active_messaging::LamellarArcLocalAm;
use crate::array::iterator::consumer::*;
use crate::array::iterator::distributed_iterator::DistributedIterator;
use crate::array::iterator::private::*;

use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::barrier::BarrierHandle;
use crate::darc::DarcMode;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::LamellarTask;
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
    type Handle = InnerDistIterCountHandle;
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
        InnerDistIterCountHandle {
            reqs,
            team,
            state: InnerState::ReqsPending(0),
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

//#[doc(hidden)]
#[pin_project]
pub(crate) struct InnerDistIterCountHandle {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<usize>>,
    team: Pin<Arc<LamellarTeamRT>>,
    state: InnerState,
}

enum InnerState {
    ReqsPending(usize),
    Counting(Pin<Box<dyn Future<Output = usize> + Send>>),
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

impl InnerDistIterCountHandle {
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

impl Future for InnerDistIterCountHandle {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            InnerState::ReqsPending(cnt) => {
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
                        *this.state = InnerState::Counting(global_cnt);
                        Poll::Pending
                    }
                }
            }
            InnerState::Counting(global_cnt) => {
                let count = ready!(Future::poll(global_cnt.as_mut(), cx));
                Poll::Ready(count)
            }
        }
    }
}

//#[doc(hidden)]
#[async_trait]
impl LamellarRequest for InnerDistIterCountHandle {
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

#[pin_project]
pub struct DistIterCountHandle {
    team: Pin<Arc<LamellarTeamRT>>,
    #[pin]
    state: State,
}

impl DistIterCountHandle {
    pub(crate) fn new(
        barrier_handle: BarrierHandle,
        inner: Pin<Box<dyn Future<Output = InnerDistIterCountHandle> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        Self {
            team: array.data.team.clone(),
            state: State::Barrier(barrier_handle, inner),
        }
    }

    /// This method will block until the associated Count operation completes and returns the result
    pub fn block(self) -> usize {
        self.team.clone().block_on(self)
    }

    /// This method will spawn the associated Count Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(self) -> LamellarTask<usize> {
        self.team.clone().scheduler.spawn_task(self)
    }
}

#[pin_project(project = StateProj)]
enum State {
    Barrier(
        #[pin] BarrierHandle,
        Pin<Box<dyn Future<Output = InnerDistIterCountHandle> + Send>>,
    ),
    Reqs(#[pin] InnerDistIterCountHandle),
}
impl Future for DistIterCountHandle {
    type Output = usize;
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
impl LamellarRequest for DistIterCountHandle {
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
