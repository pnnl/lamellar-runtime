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
pub(crate) struct Sum<I> {
    pub(crate) iter: I,
}

impl<I: IterClone> IterClone for Sum<I> {
    fn iter_clone(&self, _: Sealed) -> Self {
        Sum {
            iter: self.iter.iter_clone(Sealed),
        }
    }
}

impl<I> IterConsumer for Sum<I>
where
    I: LocalIterator + 'static,
    I::Item: SyncSend + std::iter::Sum,
{
    type AmOutput = I::Item;
    type Output = I::Item;
    type Item = I::Item;
    type Handle = LocalIterSumHandle<I::Item>;
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
            iter: self.iter_clone(Sealed),
            schedule,
        })
    }
    fn create_handle(
        self,
        _team: Pin<Arc<LamellarTeamRT>>,
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        LocalIterSumHandle {
            reqs,
            state: State::ReqsPending(None),
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

//#[doc(hidden)]
#[pin_project]
pub struct LocalIterSumHandle<T> {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<T>>,
    state: State<T>,
}

enum State<T> {
    ReqsPending(Option<T>),
}

impl<T> Future for LocalIterSumHandle<T>
where
    T: SyncSend + std::iter::Sum + for<'a> std::iter::Sum<&'a T> + 'static,
{
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        match &mut this.state {
            State::ReqsPending(local_sum) => {
                while let Some(mut req) = this.reqs.pop_front() {
                    if !req.ready_or_set_waker(cx.waker()) {
                        this.reqs.push_front(req);
                        return Poll::Pending;
                    }
                    match local_sum {
                        Some(sum) => {
                            *sum = [sum, &req.val()].into_iter().sum::<T>();
                        }
                        None => {
                            *local_sum = Some(req.val());
                        }
                    }
                }

                Poll::Ready(local_sum.take().expect("Value should be Present"))
            }
        }
    }
}

//#[doc(hidden)]
impl<T> LamellarRequest for LocalIterSumHandle<T>
where
    T: SyncSend + std::iter::Sum + for<'a> std::iter::Sum<&'a T> + 'static,
{
    fn blocking_wait(mut self) -> Self::Output {
        self.reqs
            .drain(..)
            .map(|req| req.blocking_wait())
            .sum::<Self::Output>()
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
        self.reqs.iter().map(|req| req.val()).sum::<Self::Output>()
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct SumAm<I> {
    pub(crate) iter: Sum<I>,
    pub(crate) schedule: IterSchedule,
}

impl<I: IterClone> IterClone for SumAm<I> {
    fn iter_clone(&self, _: Sealed) -> Self {
        SumAm {
            iter: self.iter.iter_clone(Sealed),
            schedule: self.schedule.clone(),
        }
    }
}

#[lamellar_impl::rt_am_local]
impl<I> LamellarAm for SumAm<I>
where
    I: LocalIterator + 'static,
    I::Item: SyncSend + std::iter::Sum,
{
    async fn exec(&self) -> Option<I::Item> {
        let iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        iter.sum::<I::Item>()
    }
}

// #[lamellar_impl::rt_am_local]
// impl<I> LamellarAm for Sum<I>
// where
//     I: LocalIterator + 'static,
//     I::Item: std::iter::Sum + SyncSend,
// {
//     async fn exec(&self) -> I::Item {
//         let mut iter = self.iter.init(self.start_i, self.end_i - self.start_i);
//         // println!("for each static thread {:?} {} {} {}",std::thread::current().id(),self.start_i, self.end_i, self.end_i - self.start_i);
//         // let mut cnt = 0;
//         let mut sum;
//         if let Some(elem) = iter.next() {
//             sum = elem;
//             while let Some(elem) = iter.next() {
//                 sum += elem;
//             }
//         }
//         else {
//             // sum = I::Item::default();
//         }
//         sum
//         // println!("thread {:?} elems processed {:?}",std::thread::current().id(), cnt);
//     }
// }
