use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::local_iterator::LocalIterator;
use crate::array::iterator::{private::*, IterRequest};
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::LamellarTeamRT;

use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;

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
        reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>,
    ) -> Box<dyn IterRequest<Output = Self::Output>> {
        Box::new(LocalIterReduceHandle { op: self.op, reqs })
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

// #[derive(Clone, Debug)]
// pub struct ReduceAsync<I, F, Fut> {
//     pub(crate) iter: I,
//     pub(crate) op: F,
//     pub(crate) _phantom: PhantomData<Fut>,
// }

// impl<I, F, Fut> IterConsumer for ReduceAsync<I, F, Fut>
// where
//     I: LocalIterator + 'static,
//     I::Item: SyncSend,
//     F: Fn(I::Item, I::Item) -> Fut + SyncSend + Clone + 'static,
//     Fut: Future<Output = I::Item> + SyncSend + Clone + 'static,
// {
//     type AmOutput = Option<I::Item>;
//     type Output = Option<I::Item>;
//     fn into_am(self, schedule: IterSchedule) -> LamellarArcLocalAm {
//         Arc::new(ReduceAsyncAm {
//             iter: self.iter,
//             op: self.op,
//             schedule,
//             _phantom: self._phantom,
//         })
//     }
//     fn create_handle(
//         self,
//         team: Pin<Arc<LamellarTeamRT>>,
//         reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>,
//     ) -> Box<dyn IterRequest<Output = Self::Output>> {
//         Box::new(LocalIterReduceHandle { op: self.op, reqs })
//     }
//     fn max_elems(&self, in_elems: usize) -> usize {
//         self.iter.elems(in_elems)
//     }
// }

#[doc(hidden)]
pub struct LocalIterReduceHandle<T, F> {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = Option<T>>>>,
    pub(crate) op: F,
}

#[doc(hidden)]
#[async_trait]
impl<T, F> IterRequest for LocalIterReduceHandle<T, F>
where
    T: SyncSend,
    F: Fn(T, T) -> T + SyncSend + 'static,
{
    type Output = Option<T>;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        futures::future::join_all(self.reqs.drain(..).map(|req| req.into_future()))
            .await
            .into_iter()
            .filter_map(|res| res)
            .reduce(self.op)
    }
    fn wait(mut self: Box<Self>) -> Self::Output {
        self.reqs
            .drain(..)
            .filter_map(|req| req.get())
            .reduce(self.op)
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
