use crate::array::iterator::local_iterator::*;

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct Sum<I>
where
    I: LocalIterator
{
    pub(crate) iter: I,
    pub(crate) start_i: usize,
    pub(crate) end_i: usize,
}

// impl<I> Sum<I>
// where
//     I: LocalIterator,
// {
//     pub(crate) fn new(iter: I) -> Sum<I> {
//         Sum { iter }
//     }
// }

#[lamellar_impl::rt_am_local]
impl<I> LamellarAm for Sum<I>
where
    I: LocalIterator + 'static,
    I::Item: std::iter::Sum + SyncSend,
{
    async fn exec(&self) -> I::Item {
        let mut iter = self.iter.init(self.start_i, self.end_i - self.start_i);
        // println!("for each static thread {:?} {} {} {}",std::thread::current().id(),self.start_i, self.end_i, self.end_i - self.start_i);
        // let mut cnt = 0;
        let mut sum;
        if let Some(elem) = iter.next() {
            sum = elem;
            while let Some(elem) = iter.next() {
                sum += elem;
            }
        }
        else {
            // sum = I::Item::default();
        }
        sum
        // println!("thread {:?} elems processed {:?}",std::thread::current().id(), cnt);
    }
}

