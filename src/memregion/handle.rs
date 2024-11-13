use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::SharedMemoryRegion;
use crate::scheduler::LamellarTask;
use crate::warnings::RuntimeWarning;
use crate::{Dist, LamellarTeamRT};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};

#[must_use = " SharedMemoryRegion 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of creating a new [SharedMemoryRegion].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the SharedMemoryRegion's team, only returning once every PE in the team has completed the call.
///
/// # Collective Operation
/// Requires all PEs associated with the `SharedMemoryRegion` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::memregion::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let memregion: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(100).block();
/// ```
pub struct FallibleSharedMemoryRegionHandle<T: Dist> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) creation_future:
        Pin<Box<dyn Future<Output = Result<SharedMemoryRegion<T>, anyhow::Error>> + Send>>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for FallibleSharedMemoryRegionHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a FallibleSharedMemoryRegionHandle").print();
        }
    }
}

impl<T: Dist> FallibleSharedMemoryRegionHandle<T> {
    /// Used to drive creation of a new SharedMemoryRegion
    /// # Examples
    ///
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let memregion: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(100).block();
    pub fn block(mut self) -> Result<SharedMemoryRegion<T>, anyhow::Error> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "SharedMemoryRegionHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the creation of the SharedMemoryRegion on the work queue
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// /// # Examples
    ///
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let memregion_task = world.alloc_shared_mem_region::<usize>(100).spawn();
    /// // do some other work
    /// let memregion = memregion_task.block();
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<Result<SharedMemoryRegion<T>, anyhow::Error>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist> Future for FallibleSharedMemoryRegionHandle<T> {
    type Output = Result<SharedMemoryRegion<T>, anyhow::Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        this.creation_future.as_mut().poll(cx)
    }
}

#[must_use = " SharedMemoryRegion 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of creating a new [SharedMemoryRegion].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the SharedMemoryRegion's team, only returning once every PE in the team has completed the call.
///
/// # Collective Operation
/// Requires all PEs associated with the `SharedMemoryRegion` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::memregion::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let memregion: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(100).block();
/// ```
pub struct SharedMemoryRegionHandle<T: Dist> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) creation_future: Pin<Box<dyn Future<Output = SharedMemoryRegion<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for SharedMemoryRegionHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a SharedMemoryRegionHandle").print();
        }
    }
}

impl<T: Dist> SharedMemoryRegionHandle<T> {
    /// Used to drive creation of a new SharedMemoryRegion
    /// # Examples
    ///
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let memregion: SharedMemoryRegion<usize> = world.alloc_shared_mem_region(100).block();
    pub fn block(mut self) -> SharedMemoryRegion<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "SharedMemoryRegionHandle::block",
            "<handle>.spawn() or<handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the creation of the SharedMemoryRegion on the work queue
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// /// # Examples
    ///
    ///```
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let memregion_task = world.alloc_shared_mem_region::<usize>(100).spawn();
    /// // do some other work
    /// let memregion = memregion_task.block();
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<SharedMemoryRegion<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist> Future for SharedMemoryRegionHandle<T> {
    type Output = SharedMemoryRegion<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        this.creation_future.as_mut().poll(cx)
    }
}

// #[must_use = " OneSidedMemoryRegion 'new' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
// #[pin_project(PinnedDrop)]
// #[doc(alias = "Collective")]
// /// This is a handle representing the operation of creating a new [OneSidedMemoryRegion].
// /// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
// /// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the OneSidedMemoryRegion's team, only returning once every PE in the team has completed the call.
// ///
// /// # Collective Operation
// /// Requires all PEs associated with the `OneSidedMemoryRegion` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
// ///
// /// # Examples
// /// ```
// /// use lamellar::array::prelude::*;
// ///
// /// let world = LamellarWorldBuilder::new().build();
// ///
// /// let array: OneSidedMemoryRegion<usize> = OneSidedMemoryRegion::new(&world,100).block();
// /// ```
// pub(crate) struct OneSidedMemoryRegionHandle<T: Dist> {
//     pub(crate) team: Pin<Arc<LamellarTeamRT>>,
//     pub(crate) launched: bool,
//     #[pin]
//     pub(crate) creation_future:
//         Pin<Box<dyn Future<Output = Result<OneSidedMemoryRegionHandle<T>, anyhow::Error>> + Send>>,
// }

// #[pinned_drop]
// impl<T: Dist> PinnedDrop for OneSidedMemoryRegionHandle<T> {
//     fn drop(self: Pin<&mut Self>) {
//         if !self.launched {
//             RuntimeWarning::DroppedHandle("a OneSidedMemoryRegionHandle").print();
//         }
//     }
// }

// impl<T: Dist> OneSidedMemoryRegionHandle<T> {
//     /// Used to drive creation of a new OneSidedMemoryRegion
//     /// # Examples
//     ///
//     ///```
//     /// use lamellar::array::prelude::*;
//     ///
//     /// let world = LamellarWorldBuilder::new().build();
//     /// let array: OneSidedMemoryRegion<usize> = OneSidedMemoryRegion::new(&world,100).block();
//     pub fn block(mut self) -> Result<OneSidedMemoryRegionHandle<T>, anyhow::Error> {
//         self.launched = true;
//         RuntimeWarning::BlockingCall(
//             "OneSidedMemoryRegionHandle::block",
//             "<handle>.spawn() or<handle>.await",
//         )
//         .print();
//         self.team.clone().block_on(self)
//     }

//     /// This method will spawn the creation of the OneSidedMemoryRegion on the work queue
//     ///
//     /// This function returns a handle that can be used to wait for the operation to complete
//     /// /// # Examples
//     ///
//     ///```
//     /// use lamellar::array::prelude::*;
//     ///
//     /// let world = LamellarWorldBuilder::new().build();
//     /// let array_task: OneSidedMemoryRegion<usize> = OneSidedMemoryRegion::new(&world,100).spawn();
//     /// // do some other work
//     /// let array = array_task.block();
//     #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
//     pub fn spawn(mut self) -> LamellarTask<Result<OneSidedMemoryRegionHandle<T>, anyhow::Error>> {
//         self.launched = true;
//         self.team.clone().spawn(self)
//     }
// }

// impl<T: Dist> Future for OneSidedMemoryRegionHandle<T> {
//     type Output = Result<OneSidedMemoryRegionHandle<T>, anyhow::Error>;
//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         self.launched = true;
//         let mut this = self.project();
//         this.creation_future.as_mut().poll(cx)
//     }
// }
