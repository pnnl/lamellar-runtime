use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::local_iterator::LocalIterator;
use crate::array::iterator::private::*;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;

use futures_util::Future;
use pin_project::pin_project;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

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
    I: LocalIterator + 'static,
    I::Item: SyncSend,
    F: Fn(I::Item, I::Item) -> I::Item + SyncSend + Clone + 'static,
{
    type AmOutput = Option<I::Item>;
    type Output = Option<I::Item>;
    type Item = I::Item;
    type Handle = LocalIterReduceHandle<I::Item, F>;
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
        _team: Pin<Arc<LamellarTeamRT>>,
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        LocalIterReduceHandle {
            op: self.op,
            reqs,
            state: State::ReqsPending(None),
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[doc(hidden)]
#[pin_project]
pub struct LocalIterReduceHandle<T, F> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<Option<T>>>,
    pub(crate) op: F,
    state: State<T>,
}

enum State<T> {
    ReqsPending(Option<T>),
}

impl<T, F> Future for LocalIterReduceHandle<T, F>
where
    T: SyncSend + Copy + 'static,
    F: Fn(T, T) -> T + SyncSend + 'static,
{
    type Output = Option<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            State::ReqsPending(val) => {
                while let Some(mut req) = this.reqs.pop_front() {
                    if !req.ready_or_set_waker(cx.waker()) {
                        this.reqs.push_front(req);
                        return Poll::Pending;
                    }
                    match val {
                        None => *val = req.val(),
                        Some(val1) => {
                            if let Some(val2) = req.val() {
                                *val = Some((this.op)(*val1, val2));
                            }
                        }
                    }
                }
                Poll::Ready(*val)
            }
        }
    }
}

#[doc(hidden)]
impl<T, F> LamellarRequest for LocalIterReduceHandle<T, F>
where
    T: SyncSend + Copy + 'static,
    F: Fn(T, T) -> T + SyncSend + Clone + 'static,
{
    fn blocking_wait(mut self) -> Self::Output {
        self.reqs
            .drain(..)
            .filter_map(|req| req.blocking_wait())
            .reduce(self.op)
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
        self.reqs
            .iter()
            .filter_map(|req| req.val())
            .reduce(self.op.clone())
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
    I: LocalIterator + 'static,
    I::Item: SyncSend,
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
// pub(crate) struct ReduceAsyncAm<I, F, Fut> {
//     pub(crate) op: F,
//     pub(crate) iter: I,
//     pub(crate) schedule: IterSchedule,
//     pub(crate) _phantom: PhantomData<Fut>
// }

// #[lamellar_impl::rt_am_local]
// impl<I, F, Fut> LamellarAm for ReduceAsyncAm<I, F, Fut>
// where
//     I: LocalIterator + 'static,
//     I::Item: SyncSend,
//     F: Fn(I::Item, I::Item) -> Fut + SyncSend + Clone + 'static,
//     Fut: Future<Output = I::Item> + SyncSend + Clone + 'static,
// {
//     async fn exec(&self) -> Option<I::Item> {
//         let mut iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
//         let mut accum = iter.next();
//         while let Some(elem) = iter.next() {
//             accum = Some((self.op)(accum.unwrap(), elem).await);
//             // cnt += 1;
//         }
//         accum
//         // println!("thread {:?} elems processed {:?}",std::thread::current().id(), cnt);
//     }
// }
