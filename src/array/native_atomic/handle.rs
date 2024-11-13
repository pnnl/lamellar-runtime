use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{ArrayOps, NativeAtomicArray, NativeAtomicType};
use crate::warnings::RuntimeWarning;
use crate::{Dist, LamellarTeamRT, UnsafeArray};

use futures_util::{ready, Future};
use pin_project::{pin_project, pinned_drop};

#[must_use = " NativeAtomicArray 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
pub(crate) struct NativeAtomicArrayHandle<T: Dist + ArrayOps + 'static> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) creation_future: Pin<Box<dyn Future<Output = UnsafeArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist + ArrayOps + 'static> PinnedDrop for NativeAtomicArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a NativeAtomicArrayHandle").print();
        }
    }
}


impl<T: Dist + ArrayOps + 'static> Future for NativeAtomicArrayHandle<T> {
    type Output = NativeAtomicArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        let array = ready!(this.creation_future.as_mut().poll(cx));
        Poll::Ready(NativeAtomicArray {
            array,
            orig_t: NativeAtomicType::of::<T>(),
        })
    }
}
