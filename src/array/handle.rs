use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_util::Future;

use pin_project::{pin_project, pinned_drop};

use crate::{scheduler::LamellarTask, warnings::RuntimeWarning, Dist, LamellarTeamRT};

use super::{AtomicArray, GlobalLockArray, LocalLockArray, ReadOnlyArray, UnsafeArray};

// #[pin_project(project = InnerRdmaHandleProj)]
// pub(crate) enum InnerRdmaHandle<T: Dist> {
//     Am(VecDeque<AmHandle<()>>),
//     Rdma(VecDeque<RdmaHandle<T>>),
//     RdmaCyclicGet(
//         VecDeque<(
//             RdmaGetBufferHandle<T>,
//             LamellarMemoryRegion<T>,
//             Enumerate<StepBy<Range<usize>>>,
//         )>,
//     ),
//     Spawned(#[pin] FuturesUnordered<LamellarTask<()>>),
// }

// impl<T: Dist> InnerRdmaHandle<T> {
//     pub(crate) fn new(op: &ArrayRdmaCmd, cyclic: bool) -> Self {
//         match op {
//             ArrayRdmaCmd::PutAm => InnerRdmaHandle::Am(VecDeque::new()),
//             ArrayRdmaCmd::GetAm => InnerRdmaHandle::Am(VecDeque::new()),
//             ArrayRdmaCmd::Put => InnerRdmaHandle::Rdma(VecDeque::new()),
//             ArrayRdmaCmd::Get(_) => {
//                 if cyclic {
//                     InnerRdmaHandle::RdmaCyclicGet(VecDeque::new())
//                 } else {
//                     InnerRdmaHandle::Rdma(VecDeque::new())
//                 }
//             }
//         }
//     }
//     pub(crate) fn from_am_reqs(am_reqs: VecDeque<AmHandle<()>>) -> Self {
//         InnerRdmaHandle::Am(am_reqs)
//     }
//     pub(crate) fn add_am(&mut self, am: AmHandle<()>) {
//         match self {
//             InnerRdmaHandle::Am(reqs) => reqs.push_back(am),
//             InnerRdmaHandle::Rdma(_) => panic!("cannot add am to rdma future"),
//             InnerRdmaHandle::RdmaCyclicGet(_) => panic!("cannot add am to rdma cyclic get future"),
//             InnerRdmaHandle::Spawned(_) => panic!("cannot add am to spawned task"),
//         }
//     }
//     pub(crate) fn add_rdma(&mut self, rdma: RdmaHandle<T>) {
//         match self {
//             InnerRdmaHandle::Am(_) => panic!("cannot add rdma to am future"),
//             InnerRdmaHandle::Rdma(reqs) => reqs.push_back(rdma),
//             InnerRdmaHandle::RdmaCyclicGet(_) => {
//                 panic!("cannot add rdma to rdma cyclic get future")
//             }
//             InnerRdmaHandle::Spawned(_) => panic!("cannot add rdma to spawned task"),
//         }
//     }
//     pub(crate) fn add_rdma_cyclic_get(
//         &mut self,
//         rdma: RdmaGetBufferHandle<T>,
//         buf: LamellarMemoryRegion<T>,
//         elems: Enumerate<StepBy<Range<usize>>>,
//     ) {
//         match self {
//             InnerRdmaHandle::Am(_) => panic!("cannot add rdma cyclic get to am future"),
//             InnerRdmaHandle::Rdma(_) => panic!("cannot add rdma cyclic get to rdma"),
//             InnerRdmaHandle::RdmaCyclicGet(reqs) => reqs.push_back((rdma, buf, elems)),
//             InnerRdmaHandle::Spawned(_) => panic!("cannot add rdma cyclic get to spawned task"),
//         }
//     }
//     pub(crate) fn drain(&mut self) {
//         match self {
//             InnerRdmaHandle::Am(reqs) => for _ in reqs.drain(0..) {},
//             InnerRdmaHandle::Rdma(reqs) => for _ in reqs.drain(0..) {},
//             InnerRdmaHandle::RdmaCyclicGet(reqs) => for _ in reqs.drain(0..) {},
//             InnerRdmaHandle::Spawned(reqs) => reqs.clear(),
//         }
//     }
//     pub(crate) fn launch(&mut self, scheduler: &Arc<Scheduler>, am_counters: Vec<Arc<AMCounters>>) {
//         take_mut::take(self, |this| match this {
//             InnerRdmaHandle::Am(mut reqs) => {
//                 for am in reqs.iter_mut() {
//                     am.launch();
//                 }
//                 InnerRdmaHandle::Am(reqs)
//             }
//             InnerRdmaHandle::Rdma(mut reqs) => {
//                 let tasks = FuturesUnordered::new();
//                 for rdma in reqs.drain(..) {
//                     tasks.push(rdma.spawn());
//                 }
//                 InnerRdmaHandle::Spawned(tasks)
//             }
//             InnerRdmaHandle::RdmaCyclicGet(mut reqs) => {
//                 let tasks = FuturesUnordered::new();
//                 for (rdma, buf, elems) in reqs.drain(..) {
//                     let rdma_task = rdma.spawn();
//                     let task = scheduler.spawn_task(
//                         async move {
//                             let data = rdma_task.await;
//                             for (k, j) in elems {
//                                 unsafe {
//                                     buf.as_mut_slice()[j] = data[k];
//                                 }
//                             }
//                         },
//                         am_counters.clone(),
//                     );
//                     tasks.push(task);
//                 }
//                 InnerRdmaHandle::Spawned(tasks)
//             }
//             InnerRdmaHandle::Spawned(reqs) => InnerRdmaHandle::Spawned(reqs),
//         });
//     }
//     pub(crate) fn blocking_wait(&mut self, lamellae: &Arc<Lamellae>) {
//         match self {
//             InnerRdmaHandle::Am(reqs) => {
//                 for req in reqs.drain(..) {
//                     req.blocking_wait();
//                 }
//             }
//             InnerRdmaHandle::Rdma(reqs) => {
//                 for req in reqs.drain(..) {
//                     let _ = req.spawn();
//                 }
//                 lamellae.comm().wait(); // block until all rdma ops are complete
//             }
//             InnerRdmaHandle::RdmaCyclicGet(reqs) => {
//                 let mut mem_regions = reqs
//                     .drain(..)
//                     .map(|(rdma, buf, elems)| {
//                         let data_task = rdma.spawn();
//                         (data_task, buf, elems)
//                     })
//                     .collect::<Vec<_>>();

//                 lamellae.comm().wait(); // block until all rdma ops are complete
//                 for (data_task, buf, elems) in mem_regions.drain(..) {
//                     let data = data_task.block();
//                     for (k, j) in elems {
//                         unsafe {
//                             buf.as_mut_slice()[j] = data[k];
//                         }
//                     }
//                 }
//             }
//             InnerRdmaHandle::Spawned(_reqs) => {
//                 lamellae.comm().wait(); // block until all rdma ops are complete
//             }
//         }
//     }
// }

// /// a task handle for an array rdma (put/get) operation
// #[pin_project(PinnedDrop)]
// pub struct ArrayRdmaHandle<T: Dist> {
//     pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
//     #[pin]
//     pub(crate) reqs: InnerRdmaHandle<T>,
//     pub(crate) spawned: bool,
// }

// // pub(crate) enum ArrayRdmaHandleState<T: Dist> {
// //     Am(Option<LocalAmHandle<()>>),
// //     AtomicLoad(ArrayRdmaAtomicLoadHandle<T>),
// //     AtomicPut(ArrayRdmaAtomicStoreHandle<T>),
// //     RdmaGet(ArrayRdmaGetHandle<T>),
// //     RdmaPut(ArrayRdmaPutHandle<T>),
// //     Launched(LamellarTask<()>),
// // }

// #[pinned_drop]
// impl<T: Dist> PinnedDrop for ArrayRdmaHandle<T> {
//     fn drop(mut self: Pin<&mut Self>) {
//         let mut this = self.as_mut();
//         if !this.spawned {
//             RuntimeWarning::disable_warnings();
//             this.reqs.drain();
//             RuntimeWarning::enable_warnings();
//             RuntimeWarning::DroppedHandle("an ArrayRdmaHandle").print();
//         }
//     }
// }

// impl<T: Dist> ArrayRdmaHandle<T> {
//     /// This method will spawn the associated Array RDMA Operation on the work queue,
//     /// initiating the remote operation.
//     ///
//     /// This function returns a handle that can be used to wait for the operation to complete
//     #[must_use = "this function returns a future used to poll for completion. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
//     pub fn spawn(mut self) -> LamellarTask<()> {
//         // for req in self.reqs.iter_mut() {
//         //     req.launch();
//         // }
//         let team_rt = self.array.team();
//         self.reqs.launch(&team_rt.scheduler, team_rt.counters());
//         self.spawned = true;
//         self.array.team().spawn(self)
//     }

//     /// This method will block the calling thread until the associated Array RDMA Operation completes
//     pub fn block(self) -> () {
//         RuntimeWarning::BlockingCall(
//             "ArrayRdmaHandle::block",
//             "<handle>.spawn() or <handle>.await",
//         )
//         .print();
//         self.array.team().block_on(self)
//     }
// }

// impl<T: Dist> LamellarRequest for ArrayRdmaHandle<T> {
//     fn launch(&mut self) -> Self::Output {
//         // for req in self.reqs.iter_mut() {
//         //     req.launch();
//         // }
//         let team_rt = self.array.team();
//         self.reqs.launch(&team_rt.scheduler, team_rt.counters());
//         self.spawned = true;
//     }
//     fn blocking_wait(mut self) -> Self::Output {
//         self.spawned = true;
//         let team_rt = self.array.team();
//         self.reqs.blocking_wait(&team_rt.lamellae);
//         // for req in self.reqs.drain(0..) {
//         //     req.blocking_wait();
//         // }
//     }
//     fn ready_or_set_waker(&mut self, waker: &Waker) -> bool {
//         if let InnerRdmaHandle::Am(reqs) = &mut self.reqs {
//             let mut ready = true;
//             for req in reqs.iter_mut() {
//                 ready &= req.ready_or_set_waker(waker);
//             }
//             self.spawned = true;
//             ready
//         } else {
//             // TODO: maybe implement this for rdma futures as well
//             false
//         }
//     }
//     fn val(&self) -> Self::Output {
//         if let InnerRdmaHandle::Am(reqs) = &self.reqs {
//             for req in reqs.iter() {
//                 req.val();
//             }
//         }
//     }
// }

// impl<T: Dist> Future for ArrayRdmaHandle<T> {
//     type Output = ();
//     fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let team_rt = self.array.team();
//         if !self.spawned {
//             take_mut::take(&mut self.reqs, |reqs| match reqs {
//                 InnerRdmaHandle::Am(mut reqs) => {
//                     for req in reqs.iter_mut() {
//                         req.ready_or_set_waker(cx.waker());
//                     }
//                     InnerRdmaHandle::Am(reqs)
//                 }
//                 InnerRdmaHandle::Rdma(mut reqs) => {
//                     let tasks = FuturesUnordered::new();

//                     for req in reqs.drain(..) {
//                         tasks.push(req.spawn());
//                     }
//                     InnerRdmaHandle::Spawned(tasks)
//                 }
//                 InnerRdmaHandle::RdmaCyclicGet(mut reqs) => {
//                     let tasks = FuturesUnordered::new();
//                     for (rdma, buf, elems) in reqs.drain(..) {
//                         let rdma_task = rdma.spawn();
//                         let task = team_rt.scheduler.spawn_task(
//                             async move {
//                                 let data = rdma_task.await;
//                                 for (k, j) in elems {
//                                     unsafe {
//                                         buf.as_mut_slice()[j] = data[k];
//                                     }
//                                 }
//                             },
//                             team_rt.counters(),
//                         );
//                         tasks.push(task);
//                     }
//                     InnerRdmaHandle::Spawned(tasks)
//                 }
//                 InnerRdmaHandle::Spawned(reqs) => InnerRdmaHandle::Spawned(reqs),
//             });
//             self.spawned = true;
//         }
//         let this = self.project();

//         match this.reqs.project() {
//             InnerRdmaHandleProj::Am(reqs) => {
//                 while let Some(mut req) = reqs.pop_front() {
//                     if !req.ready_or_set_waker(cx.waker()) {
//                         reqs.push_front(req);
//                         return Poll::Pending;
//                     }
//                 }
//             }
//             InnerRdmaHandleProj::Rdma(_reqs) => {
//                 panic!("Rdma should already have been converted to spawned");
//             }
//             InnerRdmaHandleProj::RdmaCyclicGet(_reqs) => {
//                 panic!("RdmaCyclicGet should already have been converted to spawned");
//             }
//             InnerRdmaHandleProj::Spawned(mut reqs) => {
//                 while let Poll::Ready(req) = reqs.as_mut().poll_next(cx) {
//                     if req.is_none() {
//                         return Poll::Ready(());
//                     }
//                 }
//                 return Poll::Pending;
//             }
//         }

//         Poll::Ready(())
//     }
// }

// pub(crate) struct ArrayRdmaAtomicLoadHandle<T: Dist> {
//     pub(crate) array: NativeAtomicArray<T>,
//     pub(crate) index: usize,
// }

// impl<T: Dist> ArrayRdmaAtomicLoadHandle<T> {
//     fn spawn(&self, _buf: OneSidedMemoryRegion<T>) -> LamellarTask<()> {
//         // self.array.network_atomic_load(self.index, &buf);
//         let team = self.array.team_rt();
//         team.clone().spawn(async move {
//             team.lamellae.comm().wait();
//         })
//     }

//     fn block(&self, _buf: OneSidedMemoryRegion<T>) {
//         // self.array.network_iatomic_load(self.index, &buf);
//         // let team = self.array.team_rt().lamellae().wait;
//         // team.clone().spawn(async move {
//         //     team.lamellae.comm().wait();
//         // })
//     }
// }

// pub(crate) struct ArrayRdmaNetworkAtomicLoadHandle<T: Dist> {
//     pub(crate) array: NetworkAtomicArray<T>,
//     pub(crate) index: usize,
// }

// impl<T: Dist> ArrayRdmaNetworkAtomicLoadHandle<T> {
//     fn spawn(&self, _buf: OneSidedMemoryRegion<T>) -> LamellarTask<()> {
//         // self.array.network_atomic_load(self.index, &buf);
//         let team = self.array.team_rt();
//         team.clone().spawn(async move {
//             team.lamellae.comm().wait();
//         })
//     }

//     fn block(&self, _buf: OneSidedMemoryRegion<T>) {
//         // self.array.network_iatomic_load(self.index, &buf);
//         // let team = self.array.team_rt().lamellae().wait;
//         // team.clone().spawn(async move {
//         //     team.lamellae.comm().wait();
//         // })
//     }
// }

// pub(crate) struct ArrayRdmaAtomicStoreHandle<T: Dist> {
//     pub(crate) array: NativeAtomicArray<T>,
//     pub(crate) index: usize,
// }

// impl<T: Dist> ArrayRdmaAtomicStoreHandle<T> {
//     fn exec(&self, buf: OneSidedMemoryRegion<T>) -> LamellarTask<()> {
//         self.array.network_atomic_store(self.index, &buf);
//         let team = self.array.team_rt();
//         team.clone().spawn(async move {
//             team.lamellae.comm().wait();
//         })
//     }
// }

// pub(crate) struct ArrayRdmaGetHandle<T: Dist> {
//     pub(crate) array: UnsafeArray<T>,
//     pub(crate) index: usize,
// }

// impl<T: Dist> ArrayRdmaGetHandle<T> {
//     fn spawn(&self, buf: OneSidedMemoryRegion<T>) -> LamellarTask<()> {
//         unsafe { self.array.get(self.index, &buf).spawn() }
//     }

//     fn block(&self, buf: OneSidedMemoryRegion<T>) {
//         unsafe { self.array.get(self.index, &buf).block() };
//         // team.clone().spawn(async move {
//         //     team.lamellae.comm().wait();
//         // })
//     }
// }

// pub(crate) struct ArrayRdmaPutHandle<T: Dist> {
//     pub(crate) array: UnsafeArray<T>,
//     pub(crate) index: usize,
// }

// impl<T: Dist> ArrayRdmaPutHandle<T> {
//     fn exec(&self, buf: OneSidedMemoryRegion<T>) -> LamellarTask<()> {
//         unsafe { self.array.put_unchecked(self.index, &buf) };
//         let team = self.array.team_rt();
//         team.clone().spawn(async move {
//             team.lamellae.comm().wait();
//         })
//     }
// }
/// a task handle for an array rdma 'at' operation
// #[pin_project]
// pub struct ArrayAtHandle<T: Dist> {
//     pub(crate) array: LamellarByteArray, //prevents prematurely performing a local drop
//     #[pin]
//     pub(crate) state: ArrayAtHandleState<T>,
//     // pub(crate) buf: OneSidedMemoryRegion<T>,
//     // pub(crate) spawned: bool,
// }

// #[pin_project(project = ArrayAtHandleStateProj)]
// pub(crate) enum ArrayAtHandleState<T: Dist> {
//     LocalAm(#[pin] LocalAmHandle<T>),
//     Am(#[pin] AmHandle<Vec<u8>>),
//     NetworkAtomic(#[pin] AtomicFetchOpHandle<T>),
//     Rdma(#[pin] RdmaGetHandle<T>),
//     Launched(#[pin] LamellarTask<T>),
// }

// #[pinned_drop]
// impl<T: Dist> PinnedDrop for ArrayAtHandle<T> {
//     fn drop(mut self: Pin<&mut Self>) {
//         match &mut self.state {
//             ArrayAtHandleState::Am(req) => {
//                 RuntimeWarning::disable_warnings();
//                 let _ = req.take();
//                 RuntimeWarning::enable_warnings();
//                 RuntimeWarning::DroppedHandle("an ArrayAtHandle").print();
//             }
//             _ => {}
//         }
//         // if !self.spawned {
//         //     RuntimeWarning::disable_warnings();
//         //     let _ = self.req.take();
//         //     RuntimeWarning::enable_warnings();
//         //     RuntimeWarning::DroppedHandle("an ArrayAtHandle").print();
//         // }
//     }
// }

// impl<T: Dist> ArrayAtHandle<T> {
//     /// This method will spawn the associated Array RDMA at Operation on the work queue,
//     /// initiating the remote operation.
//     ///
//     /// This function returns a handle that can be used to wait for the operation to complete
//     #[must_use = "this function returns a future used to poll for completion and retrieve the result. Call '.await' on the future otherwise, if  it is ignored (via ' let _ = *.spawn()') or dropped the only way to ensure completion is calling 'wait_all()' on the world or array. Alternatively it may be acceptable to call '.block()' instead of 'spawn()'"]
//     pub fn spawn(self) -> LamellarTask<T> {
//         match self.state {
//             ArrayAtHandleState::LocalAm(req) => req.spawn(),
//             ArrayAtHandleState::Am(req) => {
//                 let task = req.spawn();
//                 self.array.team().spawn(async move {
//                     let bytes = task.await;
//                     let result = std::mem::MaybeUninit::<T>::uninit();
//                     unsafe {
//                         std::ptr::copy_nonoverlapping(
//                             bytes.as_ptr(),
//                             result.as_ptr() as *mut u8,
//                             std::mem::size_of::<T>(),
//                         );
//                         result.assume_init()
//                     }
//                 })
//             }
//             ArrayAtHandleState::Rdma(op) => op.spawn(),
//             ArrayAtHandleState::NetworkAtomic(op) => op.spawn(),
//             _ => panic!("ArrayAtHandle should already have been spawned"),
//         }
//     }

//     /// This method will block the calling thread until the associated Array RDMA at Operation completes
//     pub fn block(self) -> T {
//         RuntimeWarning::BlockingCall("ArrayAtHandle::block", "<handle>.spawn() or <handle>.await")
//             .print();
//         match self.state {
//             ArrayAtHandleState::LocalAm(req) => req.block(),
//             ArrayAtHandleState::Am(req) => {
//                 let bytes = req.block();
//                 let result = std::mem::MaybeUninit::<T>::uninit();
//                 unsafe {
//                     std::ptr::copy_nonoverlapping(
//                         bytes.as_ptr(),
//                         result.as_ptr() as *mut u8,
//                         std::mem::size_of::<T>(),
//                     );
//                     result.assume_init()
//                 }
//             }
//             ArrayAtHandleState::Rdma(op) => op.block(),
//             ArrayAtHandleState::Launched(task) => task.block(),
//             ArrayAtHandleState::NetworkAtomic(op) => op.block(),
//         }
//     }
// }

// impl<T: Dist> Future for ArrayAtHandle<T> {
//     type Output = T;
//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let this = self.project();
//         match this.state.project() {
//             ArrayAtHandleStateProj::LocalAm(req) => req.poll(cx),
//             ArrayAtHandleStateProj::Am(req) => {
//                 let bytes = ready!(req.poll(cx));
//                 let result = std::mem::MaybeUninit::<T>::uninit();
//                 unsafe {
//                     std::ptr::copy_nonoverlapping(
//                         bytes.as_ptr(),
//                         result.as_ptr() as *mut u8,
//                         std::mem::size_of::<T>(),
//                     );
//                     Poll::Ready(result.assume_init())
//                 }
//             }
//             ArrayAtHandleStateProj::Rdma(op) => op.poll(cx),
//             ArrayAtHandleStateProj::Launched(task) => task.poll(cx),
//             ArrayAtHandleStateProj::NetworkAtomic(op) => op.poll(cx),
//         }
//     }
// }

#[must_use = " Array 'into' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of changing from a some other LamellarArray type into an [UnsafeArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Arrays's team, only returning once every PE in the team has completed the call.
///
/// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
/// pointed to object to respresented as more than one array type simultaneously (on any PE).
///
/// # Collective Operation
/// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
/// let unsafe_array = array.into_unsafe().block();
/// /* alternatively something like the following is valid as well
/// let unsafe_array = world.block_on(async move{
///     array.into_unsafe().await
/// })
///  */
/// ```
pub struct IntoUnsafeArrayHandle<T> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) outstanding_future: Pin<Box<dyn Future<Output = UnsafeArray<T>> + Send>>,
}

#[pinned_drop]
impl<T> PinnedDrop for IntoUnsafeArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a IntoUnsafeArrayHandle").print();
        }
    }
}

impl<T: Dist + 'static> IntoUnsafeArrayHandle<T> {
    /// Used to drive the cconversion of another LamellarArray into an [UnsafeArray]
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let unsafe_array = array.into_unsafe().block();
    pub fn block(mut self) -> UnsafeArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "IntoUnsafeArrayHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the conversion  into the UnsafeArray on the work queue.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    /// let unsafe_array_task = array.into_unsafe().spawn();
    /// let unsafe_array = unsafe_array_task.block();
    /// ```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<UnsafeArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T> Future for IntoUnsafeArrayHandle<T> {
    type Output = UnsafeArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.outstanding_future.as_mut().poll(cx) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(array) => Poll::Ready(array),
        }
    }
}

#[must_use = " Array 'into' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of changing from a some other LamellarArray type into an [AtomicArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Arrays's team, only returning once every PE in the team has completed the call.
///
/// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
/// pointed to object to respresented as more than one array type simultaneously (on any PE).
///
/// # Collective Operation
/// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array = GlobalLockArray::<usize>::new(&world,100, Distribution::Block).block();
/// let atomic_array = array.into_atomic().block();
/// /* alternatively something like the following is valid as well
/// let atomic_array = world.block_on(async move{
///     array.into_atomic().await
/// })
///  */
/// ```
pub struct IntoAtomicArrayHandle<T: Dist> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) outstanding_future: Pin<Box<dyn Future<Output = AtomicArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for IntoAtomicArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a IntoAtomicArrayHandle").print();
        }
    }
}

impl<T: Dist + 'static> IntoAtomicArrayHandle<T> {
    /// Used to drive the cconversion of another LamellarArray into an [AtomicArray]
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let array = GlobalLockArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let atomic_array = array.into_atomic().block();
    pub fn block(mut self) -> AtomicArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "IntoAtomicArrayHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the conversion  into the AtomicArray on the work queue.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = GlobalLockArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let atomic_array_task = array.into_atomic().spawn();
    /// let atomic_array = atomic_array_task.block();
    /// ```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<AtomicArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist> Future for IntoAtomicArrayHandle<T> {
    type Output = AtomicArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.outstanding_future.as_mut().poll(cx) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(array) => Poll::Ready(array),
        }
    }
}

#[must_use = " Array 'into' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of changing from a some other LamellarArray type into an [LocalLockArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Arrays's team, only returning once every PE in the team has completed the call.
///
/// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
/// pointed to object to respresented as more than one array type simultaneously (on any PE).
///
/// # Collective Operation
/// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
/// let local_lock_array = array.into_local_lock().block();
/// /* alternatively something like the following is valid as well
/// let local_lock_array = world.block_on(async move{
///     array.into_local_lock().await
/// })
///  */
/// ```
pub struct IntoLocalLockArrayHandle<T: Dist> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) outstanding_future: Pin<Box<dyn Future<Output = LocalLockArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for IntoLocalLockArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a IntoLocalLockArrayHandle").print();
        }
    }
}

impl<T: Dist + 'static> IntoLocalLockArrayHandle<T> {
    /// Used to drive the cconversion of another LamellarArray into an [LocalLockArray]
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let local_lock_array = array.into_local_lock().block();
    pub fn block(mut self) -> LocalLockArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "IntoLocalLockArrayHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the conversion  into the LocalLockArray on the work queue.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let local_lock_array_task = array.into_local_lock().spawn();
    /// let local_lock_array = local_lock_array_task.block();
    /// ```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<LocalLockArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist> Future for IntoLocalLockArrayHandle<T> {
    type Output = LocalLockArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.outstanding_future.as_mut().poll(cx) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(array) => Poll::Ready(array),
        }
    }
}

#[must_use = " Array 'into' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of changing from a some other LamellarArray type into an [GlobalLockArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Arrays's team, only returning once every PE in the team has completed the call.
///
/// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
/// pointed to object to respresented as more than one array type simultaneously (on any PE).
///
/// # Collective Operation
/// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
/// let global_lock_array = array.into_global_lock().block();
/// /* alternatively something like the following is valid as well
/// let global_lock_array = world.block_on(async move{
///     array.into_global_lock().await
/// })
///  */
/// ```
pub struct IntoGlobalLockArrayHandle<T: Dist> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) outstanding_future: Pin<Box<dyn Future<Output = GlobalLockArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for IntoGlobalLockArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a IntoGlobalLockArrayHandle").print();
        }
    }
}

impl<T: Dist + 'static> IntoGlobalLockArrayHandle<T> {
    /// Used to drive the cconversion of another LamellarArray into an [GlobalLockArray]
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let global_lock_array = array.into_global_lock().block();
    pub fn block(mut self) -> GlobalLockArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "IntoGlobalLockArrayHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the conversion  into the GlobalLockArray on the work queue.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let global_lock_array_task = array.into_global_lock().spawn();
    /// let global_lock_array = global_lock_array_task.block();
    /// ```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<GlobalLockArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist> Future for IntoGlobalLockArrayHandle<T> {
    type Output = GlobalLockArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.outstanding_future.as_mut().poll(cx) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(array) => Poll::Ready(array),
        }
    }
}

#[must_use = " Array 'into' handles do nothing unless polled or awaited, or 'spawn()' or 'block()' are called"]
#[pin_project(PinnedDrop)]
#[doc(alias = "Collective")]
/// This is a handle representing the operation of changing from a some other LamellarArray type into an [ReadOnlyArray].
/// This handled must either be awaited in an async context or blocked on in a non-async context for the operation to be performed.
/// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Arrays's team, only returning once every PE in the team has completed the call.
///
/// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
/// pointed to object to respresented as more than one array type simultaneously (on any PE).
///
/// # Collective Operation
/// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
///
/// # Examples
/// ```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
///
/// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
/// let read_only_array = array.into_read_only().block();
/// /* alternatively something like the following is valid as well
/// let read_only_array = world.block_on(async move{
///     array.into_read_only().await
/// })
///  */
/// ```
pub struct IntoReadOnlyArrayHandle<T: Dist> {
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) launched: bool,
    #[pin]
    pub(crate) outstanding_future: Pin<Box<dyn Future<Output = ReadOnlyArray<T>> + Send>>,
}

#[pinned_drop]
impl<T: Dist> PinnedDrop for IntoReadOnlyArrayHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a IntoReadOnlyArrayHandle").print();
        }
    }
}

impl<T: Dist + 'static> IntoReadOnlyArrayHandle<T> {
    /// Used to drive the cconversion of another LamellarArray into an [ReadOnlyArray]
    ///
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let read_only_array = array.into_read_only().block();
    pub fn block(mut self) -> ReadOnlyArray<T> {
        self.launched = true;
        RuntimeWarning::BlockingCall(
            "IntoReadOnlyArrayHandle::block",
            "<handle>.spawn() or <handle>.await",
        )
        .print();
        self.team.clone().block_on(self)
    }

    /// This method will spawn the conversion  into the ReadOnlyArray on the work queue.
    ///
    /// This function returns a handle that can be used to wait for the operation to complete
    /// # Examples
    /// ```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100, Distribution::Block).block();
    /// let read_only_array_task = array.into_read_only().spawn();
    /// let read_only_array = read_only_array_task.block();
    /// ```
    #[must_use = "this function returns a future [LamellarTask] used to poll for completion. Call '.await' on the returned future in an async context or '.block()' in a non async context.  Alternatively it may be acceptable to call '.block()' instead of 'spawn()' on this handle"]
    pub fn spawn(mut self) -> LamellarTask<ReadOnlyArray<T>> {
        self.launched = true;
        self.team.clone().spawn(self)
    }
}

impl<T: Dist> Future for IntoReadOnlyArrayHandle<T> {
    type Output = ReadOnlyArray<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.launched = true;
        let mut this = self.project();
        match this.outstanding_future.as_mut().poll(cx) {
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(array) => Poll::Ready(array),
        }
    }
}
