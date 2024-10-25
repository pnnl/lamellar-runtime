use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::array::iterator::consumer::*;
use crate::array::iterator::local_iterator::LocalIterator;
use crate::array::iterator::private::*;
use crate::array::r#unsafe::private::UnsafeArrayInner;
use crate::lamellar_request::LamellarRequest;
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;
use crate::scheduler::LamellarTask;
use crate::warnings::RuntimeWarning;

use futures_util::{ready, Future};
use pin_project::{pin_project, pinned_drop};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub(crate) struct ForEach<I, F>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    pub(crate) iter: I,
    pub(crate) op: F,
}

impl<I, F> IterClone for ForEach<I, F>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    fn iter_clone(&self, _: Sealed) -> Self {
        ForEach {
            iter: self.iter.iter_clone(Sealed),
            op: self.op.clone(),
        }
    }
}

impl<I, F> IterConsumer for ForEach<I, F>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    type AmOutput = ();
    type Output = ();
    type Item = I::Item;
    type Handle = InnerLocalIterForEachHandle;
    fn init(&self, start: usize, cnt: usize) -> Self {
        // println!("ForEach before init start {:?} cnt {:?}", start,cnt);
        let iter = ForEach {
            iter: self.iter.init(start, cnt),
            op: self.op.clone(),
        };
        // println!("ForEach after init start {:?} cnt {:?}", start,cnt);
        iter
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(ForEachAm {
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
        InnerLocalIterForEachHandle {
            reqs,
            spawned: false,
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

#[derive(Debug)]
pub(crate) struct ForEachAsync<I, F, Fut>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    pub(crate) iter: I,
    pub(crate) op: F,
    // pub(crate) _phantom: PhantomData<Fut>,
}

impl<I, F, Fut> IterClone for ForEachAsync<I, F, Fut>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn iter_clone(&self, _: Sealed) -> Self {
        ForEachAsync {
            iter: self.iter.iter_clone(Sealed),
            op: self.op.clone(),
        }
    }
}

impl<I, F, Fut> IterConsumer for ForEachAsync<I, F, Fut>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    type AmOutput = ();
    type Output = ();
    type Item = I::Item;
    type Handle = InnerLocalIterForEachHandle;
    fn init(&self, start: usize, cnt: usize) -> Self {
        ForEachAsync {
            iter: self.iter.init(start, cnt),
            op: self.op.clone(),
        }
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(ForEachAsyncAm {
            iter: self.iter_clone(Sealed),
            op: self.op.clone(),
            schedule,
            // _phantom: self._phantom.clone(),
        })
    }
    fn create_handle(
        self,
        _team: Pin<Arc<LamellarTeamRT>>,
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle {
        InnerLocalIterForEachHandle {
            reqs,
            spawned: false,
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

impl<I, F, Fut> Clone for ForEachAsync<I, F, Fut>
where
    I: LocalIterator + Clone + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn clone(&self) -> Self {
        ForEachAsync {
            iter: self.iter.clone(),
            op: self.op.clone(),
        }
    }
}

//#[doc(hidden)]
pub(crate) struct InnerLocalIterForEachHandle {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<()>>,
    spawned: bool,
}

impl Future for InnerLocalIterForEachHandle {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            for req in self.reqs.iter_mut() {
                req.ready_or_set_waker(cx.waker());
            }
            self.spawned = true;
        }
        while let Some(mut req) = self.reqs.pop_front() {
            if !req.ready_or_set_waker(cx.waker()) {
                self.reqs.push_front(req);
                return Poll::Pending;
            }
        }
        Poll::Ready(())
    }
}

//#[doc(hidden)]
#[pin_project(PinnedDrop)]
pub struct LocalIterForEachHandle {
    array: UnsafeArrayInner,
    launched: bool,
    #[pin]
    state: State,
}

#[pinned_drop]
impl PinnedDrop for LocalIterForEachHandle {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            let mut this = self.project();
            RuntimeWarning::disable_warnings();
            *this.state = State::Dropped;
            RuntimeWarning::enable_warnings();
            RuntimeWarning::DroppedHandle("a LocalIterForEachHandle").print();
        }
    }
}

impl LocalIterForEachHandle {
    pub(crate) fn new(
        reqs: Pin<Box<dyn Future<Output = InnerLocalIterForEachHandle> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        LocalIterForEachHandle {
            array: array.clone(),
            launched: false,
            state: State::Init(reqs),
        }
    }

    /// This method will block until the associated For Each operation completes and returns the result
    pub fn block(mut self) {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "LocalIterForEachHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.array.clone().block_on(self);
    }

    /// This method will spawn the associated For Each Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<()> {
        self.launched = true;
        self.array.clone().spawn(self)
    }
}

#[pin_project(project = StateProj)]
enum State {
    Init(Pin<Box<dyn Future<Output = InnerLocalIterForEachHandle> + Send>>),
    Reqs(#[pin] InnerLocalIterForEachHandle),
    Dropped,
}
impl Future for LocalIterForEachHandle {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.state.as_mut().project() {
            StateProj::Init(inner) => {
                let mut inner = ready!(Future::poll(inner.as_mut(), cx));
                match Pin::new(&mut inner).poll(cx) {
                    Poll::Ready(()) => Poll::Ready(()),
                    Poll::Pending => {
                        *this.state = State::Reqs(inner);
                        Poll::Pending
                    }
                }
            }
            StateProj::Reqs(inner) => {
                ready!(inner.poll(cx));
                Poll::Ready(())
            }
            StateProj::Dropped => panic!("called `Future::poll()` on a dropped future."),
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct ForEachAm<I, F>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    pub(crate) op: F,
    pub(crate) iter: ForEach<I, F>,
    pub(crate) schedule: IterSchedule,
}

#[lamellar_impl::rt_am_local]
impl<I, F> LamellarAm for ForEachAm<I, F>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) + SyncSend + Clone + 'static,
{
    async fn exec(&self) {
        // println!("foreacham: {:?}", std::thread::current().id());
        let mut iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        while let Some(elem) = iter.next() {
            (&self.op)(elem);
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct ForEachAsyncAm<I, F, Fut>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    pub(crate) op: F,
    pub(crate) iter: ForEachAsync<I, F, Fut>,
    pub(crate) schedule: IterSchedule,
    // pub(crate) _phantom: PhantomData<Fut>
}

#[lamellar_impl::rt_am_local]
impl<I, F, Fut> LamellarAm for ForEachAsyncAm<I, F, Fut>
where
    I: LocalIterator + 'static,
    F: Fn(I::Item) -> Fut + SyncSend + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    async fn exec(&self) {
        let mut iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        while let Some(elem) = iter.next() {
            (&self.op)(elem).await;
        }
    }
}
