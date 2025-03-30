use crate::active_messaging::LamellarArcLocalAm;
use crate::array::iterator::distributed_iterator::DistributedIterator;
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
use futures_util::{ready, Future};
use pin_project::{pin_project, pinned_drop};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub(crate) struct Sum<I> {
    pub(crate) iter: I,
}

impl<I: InnerIter> InnerIter for Sum<I> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
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
    type Handle = InnerDistIterSumHandle<I::Item>;
    fn init(&self, start: usize, cnt: usize) -> Self {
        Sum {
            iter: self.iter.init(start, cnt, Sealed),
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
        InnerDistIterSumHandle {
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
pub(crate) struct InnerDistIterSumHandle<T> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<T>>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    state: InnerState<T>,
    spawned: bool,
}

enum InnerState<T> {
    ReqsPending(Option<T>),
    Summing(Pin<Box<dyn Future<Output = T> + Send>>),
}

impl<T> InnerDistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    async fn async_reduce_remote_vals(local_sum: T, team: Pin<Arc<LamellarTeamRT>>) -> T {
        let local_sums = UnsafeArray::<T>::async_new(
            &team,
            team.num_pes,
            Distribution::Block,
            crate::darc::DarcMode::UnsafeArray,
        )
        .await;
        unsafe {
            local_sums.local_as_mut_slice()[0] = local_sum;
        };
        local_sums.async_barrier().await;
        // let buffered_iter = unsafe { local_sums.buffered_onesided_iter(self.team.num_pes) };
        // buffered_iter.into_iter().map(|&e| e).sum()
        unsafe {
            local_sums
                .sum()
                .await
                .expect("array size is greater than zero")
        }
    }

    // fn reduce_remote_vals(&self, local_sum: T, local_sums: UnsafeArray<T>) -> T {
    //     unsafe {
    //         local_sums.local_as_mut_slice()[0] = local_sum;
    //     };
    //     local_sums.tasking_barrier();
    //     // let buffered_iter = unsafe { local_sums.buffered_onesided_iter(self.team.num_pes) };
    //     // buffered_iter.into_iter().map(|&e| e).sum()
    //     unsafe {
    //         local_sums
    //             .sum()
    //             .blocking_wait()
    //             .expect("array size is greater than zero")
    //     }
    // }
}

impl<T> Future for InnerDistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            for req in self.reqs.iter_mut() {
                req.ready_or_set_waker(cx.waker());
            }
            self.spawned = true;
        }
        let mut this = self.project();
        match &mut this.state {
            InnerState::ReqsPending(local_sum) => {
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
                        *this.state = InnerState::Summing(sum);
                        Poll::Pending
                    }
                }
            }
            InnerState::Summing(sum) => {
                let local_sum = ready!(Future::poll(sum.as_mut(), cx));
                Poll::Ready(local_sum)
            }
        }
    }
}



/// This handle allows you to wait for the completion of a local iterator sum operation.
#[pin_project(PinnedDrop)]
pub struct DistIterSumHandle<T> {
    array: UnsafeArrayInner,
    launched: bool,
    #[pin]
    state: State<T>,
}

#[pinned_drop]
impl<T> PinnedDrop for DistIterSumHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            let mut this = self.project();
            RuntimeWarning::disable_warnings();
            *this.state = State::Dropped;
            RuntimeWarning::enable_warnings();
            RuntimeWarning::DroppedHandle("a DistIterSumHandle").print();
        }
    }
}

impl<T> DistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    pub(crate) fn new(
        lock: Option<IterLockFuture>,
        inner: Pin<Box<dyn Future<Output = InnerDistIterSumHandle<T>> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        let state = match lock {
            Some(inner_lock) => State::Lock(inner_lock, Some(inner)),
            None => State::Barrier(array.barrier_handle(), inner),
        };
        Self {
            array: array.clone(),
            launched: false,
            state,
        }
    }

    /// This method will block until the associated Sum operation completes and returns the result
    pub fn block(mut self) -> T {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "DistIterSumHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.array.clone().block_on(self)
    }

    /// This method will spawn the associated Sum Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<T> {
        self.launched = true;
        self.array.clone().spawn(self)
    }
}

#[pin_project(project = StateProj)]
enum State<T> {
    Lock(
        #[pin] IterLockFuture,
        Option<Pin<Box<dyn Future<Output = InnerDistIterSumHandle<T>> + Send>>>,
    ),
    Barrier(
        #[pin] BarrierHandle,
        Pin<Box<dyn Future<Output = InnerDistIterSumHandle<T>> + Send>>,
    ),
    Reqs(#[pin] InnerDistIterSumHandle<T>),
    Dropped,
}
impl<T> Future for DistIterSumHandle<T>
where
    T: Dist + ArrayOps + std::iter::Sum,
{
    type Output = T;
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
            StateProj::Dropped => panic!("called `Future::poll()` on a future that was dropped"),
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct SumAm<I> {
    pub(crate) iter: Sum<I>,
    pub(crate) schedule: IterSchedule,
}

impl<I: InnerIter> InnerIter for SumAm<I> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
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
