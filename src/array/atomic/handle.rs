use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{
    generic_atomic::GenericAtomicArrayHandle,
    native_atomic::{NativeAtomicArray, NativeAtomicArrayHandle, NativeAtomicType},
    network_atomic::{NetworkAtomicArray, NetworkAtomicArrayHandle, NetworkAtomicType},
};
use super::{ArrayOps, AtomicArray};

use crate::scheduler::LamellarTask;
use crate::warnings::RuntimeWarning;
use crate::{Dist, LamellarTeamRT};

use futures_util::{ready, Future};
use pin_project::{pin_project, pinned_drop};

#[must_use = " AtomicArray 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of creating a new [AtomicArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the AtomicArray's team, only returning once every PE in the team has completed the call.
///
/// # Collective Operation
/// Requires all PEs associated with the `AtomicArray` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array: AtomicArray<usize> = AtomicArray::new(&world,100,Distribution::Cyclic).block();
/// ```
pub struct AtomicArrayHandle<T: Dist + ArrayOps + 'static> {
    pub(crate) inner: InnerAtomicArrayHandle<T>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
}

pub(crate) enum InnerAtomicArrayHandle<T: Dist + ArrayOps + 'static> {
    Generic(GenericAtomicArrayHandle<T>),
    Native(NativeAtomicArrayHandle<T>),
    Network(NetworkAtomicArrayHandle<T>),
}
impl<T: Dist + ArrayOps + 'static> InnerAtomicArrayHandle<T> {
    fn set_launched(&mut self, val: bool) {
        match self {
            InnerAtomicArrayHandle::Generic(handle) => handle.launched = val,
            InnerAtomicArrayHandle::Native(handle) => handle.launched = val,
            InnerAtomicArrayHandle::Network(handle) => handle.launched = val,
        }
    }
}

#[pinned_drop]
impl<T: Dist + ArrayOps + 'static> PinnedDrop for AtomicArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a AtomicArrayHandle").print();
        }
    }
}

impl<T: Dist + ArrayOps + 'static> AtomicArrayHandle<T> {
    /// Used to drive creation of a new AtomicArray
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Cyclic).block();
    pub fn block(mut self) -> AtomicArray<T> {
        self.launched = true;
        self.inner.set_launched(true);
        RuntimeWarning::BlockingCall(
            "AtomicArrayHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the creation of the AtomicArray on the work queue
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array_task = AtomicArray::<usize>::new(&world,100,Distribution::Cyclic).spawn();
    /// // do some other work
    /// let array = array_task.block();
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<AtomicArray<T>> {
        self.launched = true;
        self.inner.set_launched(true);
        self.team.clone().spawn(self)
    }
}

impl<T: Dist + ArrayOps + 'static> Future for AtomicArrayHandle<T> {
    type Output = AtomicArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        self.inner.set_launched(true);
        let this = self.project();
        match this.inner {
            InnerAtomicArrayHandle::Generic(ref mut handle) => {
                let array = ready!(handle.creation_future.as_mut().poll(cx));
                Poll::Ready(AtomicArray::GenericAtomicArray(array))
            }
            InnerAtomicArrayHandle::Native(ref mut handle) => {
                let array = ready!(handle.creation_future.as_mut().poll(cx));
                Poll::Ready(AtomicArray::NativeAtomicArray(NativeAtomicArray {
                    array,
                    orig_t: NativeAtomicType::of::<T>(),
                }))
            }
            InnerAtomicArrayHandle::Network(ref mut handle) => {
                let array = ready!(handle.creation_future.as_mut().poll(cx));
                Poll::Ready(AtomicArray::NetworkAtomicArray(NetworkAtomicArray {
                    array,
                    orig_t: NetworkAtomicType::of::<T>(),
                }))
            }
        }
    }
}

impl<T: Dist + ArrayOps + 'static> Into<AtomicArrayHandle<T>> for GenericAtomicArrayHandle<T> {
    fn into(self) -> AtomicArrayHandle<T> {
        let team = self.team.clone();
        let launched = self.launched;
        AtomicArrayHandle {
            inner: InnerAtomicArrayHandle::Generic(self),
            team,
            launched,
        }
    }
}

impl<T: Dist + ArrayOps + 'static> Into<AtomicArrayHandle<T>> for NativeAtomicArrayHandle<T> {
    fn into(self) -> AtomicArrayHandle<T> {
        let team = self.team.clone();
        let launched = self.launched;
        AtomicArrayHandle {
            inner: InnerAtomicArrayHandle::Native(self),
            team,
            launched,
        }
    }
}

impl<T: Dist + ArrayOps + 'static> Into<AtomicArrayHandle<T>> for NetworkAtomicArrayHandle<T> {
    fn into(self) -> AtomicArrayHandle<T> {
        let team = self.team.clone();
        let launched = self.launched;
        AtomicArrayHandle {
            inner: InnerAtomicArrayHandle::Network(self),
            team,
            launched,
        }
    }
}
