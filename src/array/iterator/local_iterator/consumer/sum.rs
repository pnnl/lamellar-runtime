use crate::array::iterator::consumer::*;
use crate::array::iterator::IterRequest;
use crate::array::iterator::local_iterator::{LocalIterator};
use crate::active_messaging::{SyncSend,LamellarArcLocalAm};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::LamellarTeamRT;

use async_trait::async_trait;
use futures::Future;
use core::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct Sum<I>
{
    pub(crate) iter: I,
}

impl<I> IterConsumer for Sum<I>
where
    I: LocalIterator + 'static,
    I::Item: SyncSend + std::iter::Sum,
{
    type AmOutput = I::Item;
    type Output = I::Item;
    type Item = I::Item;
    fn init(&self, start: usize, cnt: usize) -> Self{
        Sum{
            iter: self.iter.init(start,cnt),
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
        Box::new(LocalIterSumHandle { reqs })
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[doc(hidden)]
pub struct LocalIterSumHandle<T> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = T>>>,
}


#[doc(hidden)]
#[async_trait]
impl<T> IterRequest for LocalIterSumHandle<T>
where
    T: SyncSend + std::iter::Sum,
{
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        futures::future::join_all(self.reqs
            .drain(..)
            .map(|req| req.into_future()))
            .await
            .into_iter()
            .sum::<Self::Output>()
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        self.reqs
            .drain(..)
            .map(|req| req.get())
            .sum::<Self::Output>()
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
    I:  LocalIterator + 'static,
    I::Item: SyncSend + std::iter::Sum,
{
    async fn exec(&self) -> Option<I::Item> {
        let mut iter = self.schedule.init_iter(self.iter.clone());
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

