use crate::array::iterator::local_iterator::*;


#[doc(hidden)]
pub struct LocalIterCountHandle {
    pub(crate) reqs: Vec<Box<dyn LamellarRequest<Output = usize>>>,
}


#[doc(hidden)]
#[async_trait]
impl LocalIterRequest for LocalIterCountHandle{
    type Output = usize;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        futures::future::join_all(self.reqs.drain(..).map(|req| req.into_future())).await.into_iter().sum::<usize>()
    }
    fn wait(mut self: Box<Self>)  -> Self::Output {
        self.reqs.drain(..).map(|req| req.get()).into_iter().sum::<usize>()
    }
}


#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct CountStatic<I>{
    pub(crate) schedule: IterSchedule<I>
}

#[lamellar_impl::rt_am_local]
impl<I> LamellarAm for CountStatic<I>
where
    I: LocalIterator + 'static,
{
    async fn exec(&self) -> I::Item{
        let mut iter = self.schedule.into_iter();
        let mut count = 0;
        while let Some(_) = iter.next() {
            count += 1;
        }
        count
    }
}



