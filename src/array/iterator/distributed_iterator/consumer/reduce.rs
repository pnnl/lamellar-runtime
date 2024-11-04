use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::distributed_iterator::DistributedIterator;
use crate::array::iterator::one_sided_iterator::OneSidedIterator;
use crate::array::iterator::private::*;
use crate::array::iterator::{consumer::*, IterLockFuture};
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::{ArrayOps, Distribution, UnsafeArray};
use crate::barrier::BarrierHandle;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::LamellarTask;
use crate::warnings::RuntimeWarning;
use crate::Dist;

use futures_util::{ready, Future, StreamExt};
use pin_project::{pin_project, pinned_drop};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub(crate) struct Reduce<I, F> {
    pub(crate) iter: I,
    pub(crate) op: F,
}

impl<I: InnerIter, F: Clone> InnerIter for Reduce<I, F> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
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
    type Handle = InnerDistIterReduceHandle<I::Item, F>;
    fn init(&self, start: usize, cnt: usize) -> Self {
        Reduce {
            iter: self.iter.init(start, cnt, Sealed),
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
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        InnerDistIterReduceHandle {
            op: self.op,
            reqs,
            team,
            state: InnerState::ReqsPending(None),
            spawned: false,
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}
//#[doc(hidden)]
#[pin_project]
pub(crate) struct InnerDistIterReduceHandle<T, F> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<Option<T>>>,
    pub(crate) op: F,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    state: InnerState<T>,
    spawned: bool,
}

enum InnerState<T> {
    ReqsPending(Option<T>),
    Reducing(Pin<Box<dyn Future<Output = Option<T>> + Send + 'static>>),
}

impl<T, F> InnerDistIterReduceHandle<T, F>
where
    T: Dist + ArrayOps,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    async fn async_reduce_remote_vals(
        local_val: T,
        team: Pin<Arc<LamellarTeamRT>>,
        op: F,
    ) -> Option<T> {
        let local_vals =
            UnsafeArray::<T>::async_new(&team, team.num_pes, Distribution::Block).await;
        unsafe {
            local_vals.local_as_mut_slice()[0] = local_val;
        };
        local_vals.async_barrier().await;
        let buffered_iter = unsafe { local_vals.buffered_onesided_iter(team.num_pes) };
        let mut stream = buffered_iter.into_stream();
        let first = stream.next().await?;

        Some(
            stream
                .fold(*first, |a, &b| {
                    let val = op(a, b);
                    async move { val }
                })
                .await,
        )
    }

    fn reduce_remote_vals(&self, local_val: T) -> Option<T> {
        self.team.tasking_barrier();
        let local_vals = UnsafeArray::<T>::new(&self.team, self.team.num_pes, Distribution::Block);
        unsafe {
            local_vals.local_as_mut_slice()[0] = local_val;
        };
        local_vals.tasking_barrier();
        let buffered_iter = unsafe { local_vals.buffered_onesided_iter(self.team.num_pes) };
        buffered_iter
            .into_iter()
            .map(|&x| x)
            .reduce(self.op.clone())
    }
}

impl<T, F> Future for InnerDistIterReduceHandle<T, F>
where
    T: Dist + ArrayOps,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    type Output = Option<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            for req in self.reqs.iter_mut() {
                req.ready_or_set_waker(cx.waker());
            }
            self.spawned = true;
        }
        let mut this = self.project();
        match &mut this.state {
            InnerState::ReqsPending(mut val) => {
                while let Some(mut req) = this.reqs.pop_front() {
                    if !req.ready_or_set_waker(cx.waker()) {
                        this.reqs.push_front(req);
                        return Poll::Pending;
                    }
                    match val {
                        None => val = req.val(),
                        Some(val1) => {
                            if let Some(val2) = req.val() {
                                val = Some((this.op)(val1, val2));
                            }
                        }
                    }
                }
                if let Some(val) = val {
                    let mut reducing = Box::pin(Self::async_reduce_remote_vals(
                        val.clone(),
                        this.team.clone(),
                        this.op.clone(),
                    ));
                    match Future::poll(reducing.as_mut(), cx) {
                        Poll::Ready(val) => Poll::Ready(val),
                        Poll::Pending => {
                            *this.state = InnerState::Reducing(reducing);
                            Poll::Pending
                        }
                    }
                } else {
                    Poll::Ready(None)
                }
            }
            InnerState::Reducing(reducing) => {
                let val = ready!(Future::poll(reducing.as_mut(), cx));
                Poll::Ready(val)
            }
        }
    }
}

#[pin_project(PinnedDrop)]
pub struct DistIterReduceHandle<T, F> {
    array: UnsafeArrayInner,
    launched: bool,
    #[pin]
    state: State<T, F>,
}

#[pinned_drop]
impl<T, F> PinnedDrop for DistIterReduceHandle<T, F> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            let mut this = self.project();
            RuntimeWarning::disable_warnings();
            *this.state = State::Dropped;
            RuntimeWarning::enable_warnings();
            RuntimeWarning::DroppedHandle("a DistIterReduceHandle").print();
        }
    }
}

impl<T, F> DistIterReduceHandle<T, F>
where
    T: Dist + ArrayOps,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    pub(crate) fn new(
        lock: Option<IterLockFuture>,
        reqs: Pin<Box<dyn Future<Output = InnerDistIterReduceHandle<T, F>> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        let state = match lock {
            Some(inner_lock) => State::Lock(inner_lock, Some(reqs)),
            None => State::Barrier(array.barrier_handle(), reqs),
        };
        Self {
            array: array.clone(),
            launched: false,
            state,
        }
    }

    /// This method will block until the associated Reduce operation completes and returns the result
    pub fn block(mut self) -> Option<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "DistIterReduceHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.array.clone().block_on(self)
    }

    /// This method will spawn the associated Reduce Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<Option<T>> {
        self.launched = true;
        self.array.clone().spawn(self)
    }
}

#[pin_project(project = StateProj)]
enum State<T, F> {
    Lock(
        #[pin] IterLockFuture,
        Option<Pin<Box<dyn Future<Output = InnerDistIterReduceHandle<T, F>> + Send>>>,
    ),
    Barrier(
        #[pin] BarrierHandle,
        Pin<Box<dyn Future<Output = InnerDistIterReduceHandle<T, F>> + Send>>,
    ),
    Reqs(#[pin] InnerDistIterReduceHandle<T, F>),
    Dropped,
}
impl<T, F> Future for DistIterReduceHandle<T, F>
where
    T: Dist + ArrayOps,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    type Output = Option<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.state.as_mut().project() {
            StateProj::Lock(lock, inner) => {
                ready!(lock.poll(cx));
                let barrier = this.array.barrier_handle();
                *this.state = State::Barrier(
                    barrier,
                    inner.take().expect("reqs should still be in this state"),
                );
                cx.waker().wake_by_ref();
                Poll::Pending
            }
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
            StateProj::Dropped => panic!("called `Future::poll()` on a dropped future."),
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct ReduceAm<I, F> {
    pub(crate) op: F,
    pub(crate) iter: Reduce<I, F>,
    pub(crate) schedule: IterSchedule,
}

impl<I: InnerIter, F: Clone> InnerIter for ReduceAm<I, F> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
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
