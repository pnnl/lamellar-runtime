use crate::active_messaging::LamellarArcLocalAm;
use crate::array::iterator::local_iterator::LocalIterator;
use crate::array::iterator::{consumer::*, private::*, IterLockFuture};
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
pub(crate) struct Count<I> {
    pub(crate) iter: I,
}

impl<I: InnerIter> InnerIter for Count<I> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        Count {
            iter: self.iter.iter_clone(Sealed),
        }
    }
}

impl<I> IterConsumer for Count<I>
where
    I: LocalIterator,
{
    type AmOutput = usize;
    type Output = usize;
    type Item = I::Item;
    type Handle = InnerLocalIterCountHandle;
    fn init(&self, start: usize, cnt: usize) -> Self {
        Count {
            iter: self.iter.init(start, cnt, Sealed),
        }
    }
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm {
        Arc::new(CountAm {
            iter: self.iter_clone(Sealed),
            schedule,
        })
    }
    fn create_handle(
        self,
        _team: Pin<Arc<LamellarTeamRT>>,
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> InnerLocalIterCountHandle {
        InnerLocalIterCountHandle {
            reqs,
            state: InnerState::ReqsPending(0),
            spawned: false,
        }
    }
    fn max_elems(&self, in_elems: usize) -> usize {
        self.iter.elems(in_elems)
    }
}

//#[doc(hidden)]
#[pin_project]
pub(crate) struct InnerLocalIterCountHandle {
    pub(crate) reqs: VecDeque<TaskGroupLocalAmHandle<usize>>,
    state: InnerState,
    spawned: bool,
}

enum InnerState {
    ReqsPending(usize),
}

impl Future for InnerLocalIterCountHandle {
    type Output = usize;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.spawned {
            for req in self.reqs.iter_mut() {
                req.ready_or_set_waker(cx.waker());
            }
            self.spawned = true;
        }
        let mut this = self.project();
        match &mut this.state {
            InnerState::ReqsPending(cnt) => {
                while let Some(mut req) = this.reqs.pop_front() {
                    if !req.ready_or_set_waker(cx.waker()) {
                        this.reqs.push_front(req);
                        return Poll::Pending;
                    }
                    *cnt += req.val();
                }
                Poll::Ready(*cnt)
            }
        }
    }
}

#[pin_project(PinnedDrop)]
pub struct LocalIterCountHandle {
    array: UnsafeArrayInner,
    launched: bool,
    #[pin]
    state: State,
}

#[pinned_drop]
impl PinnedDrop for LocalIterCountHandle {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            let mut this = self.project();
            RuntimeWarning::disable_warnings();
            *this.state = State::Dropped;
            RuntimeWarning::enable_warnings();
            RuntimeWarning::DroppedHandle("a LocalIterCountHandle").print();
        }
    }
}

impl LocalIterCountHandle {
    pub(crate) fn new(
        lock: Option<IterLockFuture>,
        inner: Pin<Box<dyn Future<Output = InnerLocalIterCountHandle> + Send>>,
        array: &UnsafeArrayInner,
    ) -> Self {
        Self {
            array: array.clone(),
            launched: false,
            state: State::Init(lock, inner),
        }
    }

    /// This method will block until the associated Count operation completes and returns the result
    pub fn block(mut self) -> usize {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "LocalIterCountHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.array.clone().block_on(self)
    }

    /// This method will spawn the associated Count Operation on the work queue,
    /// initiating the remote operation.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
    pub fn spawn(mut self) -> LamellarTask<usize> {
        self.launched = true;
        self.array.clone().spawn(self)
    }
}

#[pin_project(project = StateProj)]
enum State {
    Init(
        Option<IterLockFuture>,
        Pin<Box<dyn Future<Output = InnerLocalIterCountHandle> + Send>>,
    ),
    Reqs(#[pin] InnerLocalIterCountHandle),
    Dropped,
}
impl Future for LocalIterCountHandle {
    type Output = usize;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.state.as_mut().project() {
            StateProj::Init(lock, inner) => {
                if let Some(lock) = lock {
                    ready!(lock.as_mut().poll(cx));
                }
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
            StateProj::Dropped => panic!("called `Future::poll()` on a dropped future."),
        }
    }
}

#[lamellar_impl::AmLocalDataRT(Clone)]
pub(crate) struct CountAm<I> {
    pub(crate) iter: Count<I>,
    pub(crate) schedule: IterSchedule,
}

impl<I> InnerIter for CountAm<I>
where
    I: InnerIter,
{
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        CountAm {
            iter: self.iter.iter_clone(Sealed),
            schedule: self.schedule.clone(),
        }
    }
}

#[lamellar_impl::rt_am_local]
impl<I> LamellarAm for CountAm<I>
where
    I: LocalIterator + 'static,
{
    async fn exec(&self) -> usize {
        let mut iter = self.schedule.init_iter(self.iter.iter_clone(Sealed));
        let mut count: usize = 0;
        while let Some(_) = iter.next() {
            count += 1;
        }
        // println!("count: {} {:?}", count, std::thread::current().id());
        count
    }
}
