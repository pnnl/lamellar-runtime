use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{ArrayOps, UnsafeArray};
use crate::scheduler::LamellarTask;
use crate::warnings::RuntimeWarning;
use crate::{Dist, LamellarTeamRT};

use futures_util::{ready, Future};
use pin_project::{pin_project, pinned_drop};

#[must_use = " UnsafeArray 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of creating a new [UnsafeArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the UnsafeArray's team, only returning once every PE in the team has completed the call.
///
/// # Collective Operation
/// Requires all PEs associated with the `UnsafeArray` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic).block();
/// ```
pub struct UnsafeArrayHandle<T: Dist + ArrayOps + 'static> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) creation_future: Pin<Box<dyn Future<Output = UnsafeArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist + ArrayOps + 'static> PinnedDrop for UnsafeArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a UnsafeArrayHandle").print();
        }
    }
}

impl<T: Dist + ArrayOps + 'static> UnsafeArrayHandle<T> {
    /// Used to drive creation of a new UnsafeArray
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic).block();
    pub fn block(mut self) -> UnsafeArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "UnsafeArrayHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the creation of the UnsafeArray on the work queue
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array_task: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic).spawn();
    /// // do some other work
    /// let array = array_task.block();
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<UnsafeArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist + ArrayOps + 'static> Future for UnsafeArrayHandle<T> {
    type Output = UnsafeArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        let array = ready!(this.creation_future.as_mut().poll(cx));
        Poll::Ready(array)
    }
}
