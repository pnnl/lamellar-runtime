use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::distributed_iterator::DistributedIterator;
use crate::array::iterator::one_sided_iterator::OneSidedIterator;
use crate::array::iterator::private::*;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::array::{ArrayOps, Distribution, UnsafeArray};
use crate::barrier::BarrierHandle;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;
use crate::Dist;

use futures_util::{ready, Future, StreamExt};
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

#[derive(Clone, Debug)]
pub(crate) struct Reduce<I, F> {
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
    type Handle = InnerDistIterReduceHandle<I::Item, F>;
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
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        InnerDistIterReduceHandle {
            op: self.op,
            reqs,
            team,
            state: InnerState::ReqsPending(None),
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

// #[derive(Debug)]
// pub(crate) struct ReduceAsync<I, T, F>
// where
//     I: DistributedIterator + 'static,
//     I::Item: Future<Output = T> + Send + 'static,
//     T: Dist + ArrayOps,
//     F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
// {
//     pub(crate) iter: I,
//     pub(crate) op: F,
//     // pub(crate) _phantom: PhantomData<T>,
// }

// impl<I, T, F> IterClone for ReduceAsync<I, T, F>
// where
//     I: DistributedIterator + 'static,
//     I::Item: Future<Output = T> + SyncSend + 'static,
//     T: Dist + Send + ArrayOps,
//     F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
// {
//     fn iter_clone(&self, _: Sealed) -> Self {
//         ReduceAsync {
//             iter: self.iter.iter_clone(Sealed),
//             op: self.op.clone(),
//         }
//     }
// }

// impl<I, T, F> IterConsumer for ReduceAsync<I, T, F>
// where
//     I: DistributedIterator + 'static,
//     I::Item: Future<Output = T> + Send + 'static,
//     T: Dist + Send + ArrayOps,
//     F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
// {
//     type AmOutput = Option<T>;
//     type Output = Option<T>;
//     type Item = I::Item;
//     type Handle = InnerDistIterReduceHandle<I::Item, F>;
//     fn init(&self, start: usize, cnt: usize) -> Self {
//         ReduceAsync {
//             iter: self.iter.init(start, cnt),
//             op: self.op.clone(),
//         }
//     }
//     fn next(&mut self) -> Option<Self::Item> {
//         self.iter.next()
//     }
//     fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
//         Arc::new(ReduceAsyncAm {
//             iter: self.iter_clone(Sealed),
//             op: self.op.clone(),
//             schedule,
//         })
//     }
//     fn create_handle(
//         self,
//         team: Pin<Arc<LamellarTeamRT>>,
//         reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
//     ) -> Self::Handle {
//         InnerDistIterReduceHandle {
//             op: self.op,
//             reqs,
//             team,
//             state: InnerState::ReqsPending(None),
//         }
//     }
//     fn max_elems(&self, in_elems: usize) -> usize {
//         self.iter.elems(in_elems)
//     }
// }

// impl<I, T, F> Clone for ReduceAsync<I, T, F>
// where
//     I: DistributedIterator + Clone,
//     I::Item: Future<Output = T> + Send + 'static,
//     T: Dist + Send + ArrayOps,
//     F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
// {
//     fn clone(&self) -> Self {
//         ReduceAsync {
//             iter: self.iter.clone(),
//             op: self.op.clone(),
//         }
//     }
// }

//#[doc(hidden)]
#[pin_project]
pub(crate) struct InnerDistIterReduceHandle<T, F> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<Option<T>>>,
    pub(crate) op: F,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    state: InnerState<T>,
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
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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

//#[doc(hidden)]
impl<T, F> LamellarRequest for InnerDistIterReduceHandle<T, F>
where
    T: Dist + ArrayOps,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    fn blocking_wait(mut self) -> Self::Output {
        let local_val = self
            .reqs
            .drain(..)
            .filter_map(|req| req.blocking_wait())
            .reduce(self.op.clone());
        if let Some(val) = local_val {
            self.reduce_remote_vals(val)
        } else {
            None
        }
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
        let local_val = self
            .reqs
            .iter()
            .filter_map(|req| req.val())
            .reduce(self.op.clone());
        if let Some(val) = local_val {
            self.reduce_remote_vals(val)
        } else {
            None
        }
    }
}

#[pin_project]
pub struct DistIterReduceHandle<T, F> {
    // pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<()>>,
    team: Pin<Arc<LamellarTeamRT>>,
    #[pin]
    state: State<T, F>,
}

impl<T, F> DistIterReduceHandle<T, F>
// where
//     T: Dist + Send + ArrayOps,
//     F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    pub(crate) fn new(
        barrier: BarrierHandle,
        reqs: Pin<Box<dyn Future<Output = InnerDistIterReduceHandle<T, F>> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        Self {
            team: array.data.team.clone(),
            state: State::Barrier(barrier, reqs),
        }
    }
}

#[pin_project(project = StateProj)]
enum State<T, F> {
    Barrier(
        #[pin] BarrierHandle,
        Pin<Box<dyn Future<Output = InnerDistIterReduceHandle<T, F>> + Send>>,
    ),
    Reqs(#[pin] InnerDistIterReduceHandle<T, F>),
}
impl<T, F> Future for DistIterReduceHandle<T, F>
where
    T: Dist + ArrayOps,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    type Output = Option<T>;
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
impl<T, F> LamellarRequest for DistIterReduceHandle<T, F>
where
    T: Dist + ArrayOps,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
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

// #[lamellar_impl::AmLocalDataRT(Clone)]
// pub(crate) struct ReduceAsyncAm<I, T, F>
// where
//     I: DistributedIterator + 'static,
//     I::Item: Future<Output = T> + Send + 'static,
//     T: Dist + ArrayOps,
//     F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
// {
//     pub(crate) op: F,
//     pub(crate) iter: ReduceAsync<I, T, F>,
//     pub(crate) schedule: IterSchedule,
// }

// impl<I: IterClone, T, F: Clone> IterClone for ReduceAsyncAm<I, T, F>
// where
//     I: DistributedIterator + 'static,
//     I::Item: Future<Output = T> + Send + 'static,
//     T: Dist + ArrayOps,
//     F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
// {
//     fn iter_clone(&self, _: Sealed) -> Self {
//         ReduceAsyncAm {
//             op: self.op.clone(),
//             iter: self.iter.iter_clone(Sealed),
//             schedule: self.schedule.clone(),
//         }
//     }
// }

// #[lamellar_impl::rt_am_local]
// impl<I, T, F> LamellarAm for ReduceAsyncAm<I, T, F>
// where
//     I: DistributedIterator + 'static,
//     I::Item: Future<Output = T> + Send + 'static,
//     T: Dist + Send + ArrayOps,
//     F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
// {
//     async fn exec(&self) -> Option<T> {
//         let mut iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
//         match iter.next() {
//             Some(mut accum) => {
//                 while let Some(elem) = iter.next() {
//                     accum = (self.op)(accum, elem);
//                 }
//                 Some(accum)
//             }
//             None => None,
//         }
//     }
// }
