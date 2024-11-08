use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{ArrayOps, GenericAtomicArray};
use crate::scheduler::LamellarTask;
use crate::warnings::RuntimeWarning;
use crate::{Dist, LamellarTeamRT};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};

#[must_use = " GenericAtomicArray 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
pub(crate) struct GenericAtomicArrayHandle<T: Dist + ArrayOps + 'static> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) creation_future: Pin<Box<dyn Future<Output = GenericAtomicArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist + ArrayOps + 'static> PinnedDrop for GenericAtomicArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a GenericAtomicArrayHandle").print();
        }
    }
}

impl<T: Dist + ArrayOps + 'static> GenericAtomicArrayHandle<T> {
    pub(crate) fn block(mut self) -> GenericAtomicArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "GenericAtomicArrayHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub(crate) fn spawn(mut self) -> LamellarTask<GenericAtomicArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist + ArrayOps + 'static> Future for GenericAtomicArrayHandle<T> {
    type Output = GenericAtomicArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        this.creation_future.as_mut().poll(cx)
    }
}
