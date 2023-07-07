use crate::array::iterator::consumer::*;
use crate::array::iterator::IterRequest;
use crate::array::iterator::local_iterator::{LocalIterator};
use crate::active_messaging::LamellarArcLocalAm;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_team::LamellarTeamRT;

use async_trait::async_trait;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Count<I> {
    pub(crate) iter: I,
}

impl <I> IterConsumer for Count<I>
where
    I: LocalIterator,
{
    type AmOutput = usize;
    type Output = usize;
    type Item = I::Item;
    fn init(&self, start: usize, cnt: usize) -> Self{
        self.init(start,cnt)
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm{
        Arc::new(CountAm{
            iter: self.clone(),
            schedule
        })
    }
    fn create_handle(self, team: Pin<Arc<LamellarTeamRT>>, reqs: Vec<Box<dyn LamellarRequest<Output = Self::AmOutput>>>) -> Box<dyn IterRequest<Output = Self::Output>>{
        Box::new(LocalIterCountHandle {
            reqs
        })
    }
    fn max_elems(&self, in_elems: usize) -> usize{
        self.iter.elems(in_elems)
    }
} 


#[doc(hidden)]
pub struct LocalIterCountHandle {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = usize>>>,
}


#[doc(hidden)]
#[async_trait]
impl IterRequest for LocalIterCountHandle{
    type Output = usize;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let count = futures::future::join_all(self.reqs.drain(..).map(|req| req.into_future())).await.into_iter().sum::<usize>();
        // println!("count: {} {:?}", count, std::thread::current().id());
        count
    }
    fn wait(mut self: Box<Self>)  -> Self::Output {
        self.reqs.drain(..).map(|req| req.get()).into_iter().sum::<usize>()
    }
}


#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct CountAm<I>{
    pub(crate) iter: Count<I>,
    pub(crate) schedule: IterSchedule
}

#[lamellar_impl::rt_am_local]
impl<I> LamellarAm for CountAm<I>
where
    I:  LocalIterator + 'static,
{
    async fn exec(&self) -> usize{
        let mut iter = self.schedule.init_iter(self.iter.clone());
        let mut count: usize  = 0;
        while let Some(_) = iter.next() {
            count += 1;
        }
        // println!("count: {} {:?}", count, std::thread::current().id());
        count
    }
}



