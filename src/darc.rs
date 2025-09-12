//! Distributed Atomic Reference Counter-- a distributed extension of an [`Arc`] called a [Darc][crate::darc].
//! The atomic reference counter, [`Arc`], is a backbone of safe
//! concurrent programming in Rust, and, in particular, *shared ownership*.
//!
//! The `Darc` provides a similar abstraction within a *distributed* environment.
//! - `Darc`'s have global lifetime tracking and management, meaning that the pointed to objects remain valid and accessible
//!   as long as one reference exists on any PE.
//! - Inner mutability is disallowed by default. If you need to mutate through a Darc use [`Mutex`][std::sync::Mutex], [`RwLock`][std::sync::RwLock], or one of the [`Atomic`][std::sync::atomic]
//! types. Alternatively you can also use a [`LocalRwDarc`] or [`GlobalRwDarc`].
//!
//! `Darc`'s are intended to be passed via active messages.
//! - They allow distributed
//!   accesss to and manipulation of generic Rust objects.  The inner object can exist
//!   on the Rust heap or in a registered memory region.
//! - They are instantiated in registered memory regions.
//! # Examples
//!```
//! use lamellar::active_messaging::prelude::*;
//! use lamellar::darc::prelude::*;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//! use std::sync::Arc;
//!
//! #[lamellar::AmData(Clone)]
//! struct DarcAm {
//!     counter: Darc<AtomicUsize>, //each pe has a local atomicusize
//! }
//!
//! #[lamellar::am]
//! impl LamellarAm for DarcAm {
//!     async fn exec(self) {
//!         self.counter.fetch_add(1, Ordering::SeqCst); //this only updates atomic on the executing pe
//!     }
//!  }
//!
//! fn main(){
//!     let world = LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     let num_pes = world.num_pes();
//!     let darc_counter = Darc::new(&world, AtomicUsize::new(0)).block().unwrap();
//!     let _ = world.exec_am_all(DarcAm {counter: darc_counter.clone()}).spawn();
//!     darc_counter.fetch_add(my_pe, Ordering::SeqCst);
//!     world.wait_all(); // wait for my active message to return
//!     world.barrier(); //at this point all updates will have been performed
//!     assert_eq!(darc_counter.load(Ordering::SeqCst),num_pes+my_pe); //NOTE: the value of darc_counter will be different on each PE
//! }
///```
use core::marker::PhantomData;
use futures_util::future::join_all;
use serde::{Deserialize, Deserializer};
use std::cmp::PartialEq;
use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, trace};
// use std::time::Instant;

// //use tracing::*;

use crate::{
    active_messaging::{AMCounters, RemotePtr},
    barrier::Barrier,
    env_var::config,
    lamellae::{
        AllocationType, Backend, CommAlloc, CommAllocAddr, CommAllocRdma, CommInfo, CommMem,
        CommProgress, CommSlice,
    },
    lamellar_request::LamellarRequest,
    lamellar_team::{IntoLamellarTeam, LamellarTeamRT},
    lamellar_world::LAMELLAES,
    scheduler::LamellarTask,
    warnings::RuntimeWarning,
    IdError, LamellarEnv, LamellarTeam,
};

/// prelude for the darc module
pub mod prelude;

pub(crate) mod local_rw_darc;
pub use local_rw_darc::LocalRwDarc;

pub(crate) mod global_rw_darc;
pub use global_rw_darc::GlobalRwDarc;

use self::handle::{DarcHandle, IntoGlobalRwDarcHandle, IntoLocalRwDarcHandle};

pub(crate) mod handle;

static DARC_ID: AtomicUsize = AtomicUsize::new(0);

#[repr(u64)]
#[derive(PartialEq, Copy, Clone, Debug)]
pub(crate) enum DarcMode {
    Darc,
    LocalRw,
    GlobalRw,
    UnsafeArray,
    ReadOnlyArray,
    // LocalOnlyArray,
    // AtomicArray,
    GenericAtomicArray,
    NativeAtomicArray,
    NetworkAtomicArray,
    LocalLockArray,
    GlobalLockArray,
    Dropping,
    Dropped,
    RestartDrop,
}

// impl std::fmt::Debug for DarcMode {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{:?}", *self as u64)
//     }
// }

// unsafe impl Send for DarcMode {}
// unsafe impl Sync for DarcMode {}

// impl Remote for DarcMode {}

#[lamellar_impl::AmDataRT(Debug)]
struct FinishedAm {
    cnt: usize,
    src_pe: usize,
    inner_addr: CommAllocAddr, //cant pass the darc itself cause we cant handle generics yet in lamellarAM...
}

#[lamellar_impl::rt_am]
impl LamellarAM for FinishedAm {
    async fn exec() {
        debug!("in finished! {:?}", self);
        let inner: &DarcInner<()> = unsafe { &*(self.inner_addr.as_ptr()) }; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc
                                                                             // inner.team().print_cnt();
        inner.dist_cnt.fetch_sub(self.cnt, Ordering::SeqCst);
    }
}

#[doc(hidden)]
#[repr(C)]
pub struct DarcInner<T> {
    id: usize,
    my_pe: usize,           // with respect to LamellarArch used to create this object
    num_pes: usize,         // with respect to LamellarArch used to create this object
    local_cnt: AtomicUsize, // cnt of times weve cloned for local access
    total_local_cnt: AtomicUsize,
    weak_local_cnt: AtomicUsize, // cnt of times weve cloned for local access with a weak reference
    dist_cnt: AtomicUsize,       // cnt of times weve cloned (serialized) for distributed access
    total_dist_cnt: AtomicUsize,
    ref_cnt_slice: CommSlice<usize>, // array of cnts for accesses from remote pes
    total_ref_cnt_slice: CommSlice<usize>,
    mode_slice: CommSlice<DarcMode>,
    mode_ref_cnt_slice: CommSlice<usize>,
    mode_barrier_slice: CommSlice<usize>,
    // mode_barrier_rounds: usize,
    barrier: *mut Barrier,
    am_counters: *const AMCounters,
    team: *const LamellarTeamRT,
    item: *const T,
    drop: Option<fn(&mut T) -> bool>,
    valid: AtomicBool,
}
unsafe impl<T> Send for DarcInner<T> {} //we cant create DarcInners without going through the Darc interface which enforces  Sync+Send
unsafe impl<T> Sync for DarcInner<T> {} //we cant create DarcInners without going through the Darc interface which enforces  Sync+Send

/// Distributed atomic reference counter
///
/// The atomic reference counter, [`Arc`], is a backbone of safe
/// concurrent programming in Rust, and, in particular, *shared ownership*.
///
/// The `Darc` provides a similar abstraction within a *distributed* environment.
/// - `Darc`'s have global lifetime, meaning that the pointed to objects remain valid and accessible
///   as long as one reference exists on any PE.
/// - Inner mutability is disallowed by default. If you need to mutate through a Darc use [`Mutex`][std::sync::Mutex], [`RwLock`][std::sync::RwLock], or one of the [`Atomic`][std::sync::atomic]
/// types. Alternatively you can also use a [`LocalRwDarc`] or [`GlobalRwDarc`].
///
/// `Darc`'s are intended to be passed via active messages.
/// - They allow distributed
///   accesss to and manipulation of generic Rust objects.  The inner object can exist
///   on the Rust heap or in a registered memory region.
/// - They are instantiated in registered memory regions.
///
/// # Examples
///```
/// use lamellar::active_messaging::prelude::*;
/// use lamellar::darc::prelude::*;
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
///
/// #[lamellar::AmData(Clone)]
/// struct DarcAm {
///     counter: Darc<AtomicUsize>, //each pe has a local atomicusize
/// }
///
/// #[lamellar::am]
/// impl LamellarAm for DarcAm {
///     async fn exec(self) {
///         self.counter.fetch_add(1, Ordering::SeqCst); //this only updates atomic on the executing pe
///     }
///  }
///
/// fn main(){
///     let world = LamellarWorldBuilder::new().build();
///     let my_pe = world.my_pe();
///     let num_pes = world.num_pes();
///     let darc_counter = Darc::new(&world, AtomicUsize::new(0)).block().unwrap();
///     let _ = world.exec_am_all(DarcAm {counter: darc_counter.clone()}).spawn();
///     darc_counter.fetch_add(my_pe, Ordering::SeqCst);
///     world.wait_all(); // wait for my active message to return
///     world.barrier(); //at this point all updates will have been performed
///     assert_eq!(darc_counter.load(Ordering::SeqCst),num_pes+my_pe); //NOTE: the value of darc_counter will be different on each PE
/// }
///```
pub struct Darc<T: 'static> {
    // inner: *mut DarcInner<T>,
    inner: DarcCommPtr<T>,
    src_pe: usize,
    id: usize,
}
unsafe impl<T: Sync + Send> Send for Darc<T> {}
unsafe impl<T: Sync + Send> Sync for Darc<T> {}

impl<T> LamellarEnv for Darc<T> {
    fn my_pe(&self) -> usize {
        self.inner().my_pe
    }
    fn num_pes(&self) -> usize {
        self.inner().num_pes
    }
    fn num_threads_per_pe(&self) -> usize {
        self.inner().team().num_threads_per_pe()
    }
    fn world(&self) -> Arc<LamellarTeam> {
        self.inner().team().world()
    }
    fn team(&self) -> Arc<LamellarTeam> {
        self.inner().team().team()
    }
}

impl<T: 'static> serde::Serialize for Darc<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        __NetworkDarc::from(self).serialize(serializer)
    }
}

impl<'de, T: 'static> Deserialize<'de> for Darc<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ndarc: __NetworkDarc = Deserialize::deserialize(deserializer)?;
        Ok(ndarc.into())
    }
}

//#[doc(hidden)]
/// `WeakDarc`` is a version of Darc that holds a non-owning reference to the managed object.
/// (similar to [`Weak`](std::sync::Weak)).
/// The managed object can be accessed by calling [`upgrade`](WeakDarc::upgrade), wich returns and ``Option<Darc<T>>``
///
/// A `WeakDarc` does not count toward ownership, thus it will not prevent the value stored in the allocation from being dropped,
/// and it makes no guarantees itself about the value still being present, and thus can return `None` from `upgrade()`.
/// Note that a `WeakDarc` does prevent the allocation itself from being deallocated.
///
/// The typical way to obtian a `WeakDarc` is to call [`Darc::downgrade`](Darc::downgrade).
///
/// # Examples
///```
/// use lamellar::active_messaging::prelude::*;
/// use lamellar::darc::prelude::*;
/// use std::sync::atomic::{AtomicUsize, Ordering};
/// use std::sync::Arc;
///
/// #[lamellar::AmData(Clone)]
/// struct DarcAm {
///     counter: Darc<AtomicUsize>, //each pe has a local atomicusize
/// }
///
/// #[lamellar::am]
/// impl LamellarAm for DarcAm {
///     async fn exec(self) {
///         self.counter.fetch_add(1, Ordering::SeqCst); //this only updates atomic on the executing pe
///     }
///  }
///
/// fn main(){
///     let world = LamellarWorldBuilder::new().build();
///     let my_pe = world.my_pe();
///     let num_pes = world.num_pes();
///     let darc_counter = Darc::new(&world, AtomicUsize::new(0)).block().unwrap();
///     let weak = Darc::downgrade(&darc_counter);
///     match weak.upgrade(){
///         Some(counter) => {
///             counter.fetch_add(my_pe, Ordering::SeqCst);
///         }
///         None => {
///             println!("counter is gone");
///         }   
///     }
/// }
///```
///
#[derive(Debug)]
pub struct WeakDarc<T: 'static> {
    inner: DarcCommPtr<T>,
    src_pe: usize,
}
unsafe impl<T: Send> Send for WeakDarc<T> {}
unsafe impl<T: Sync> Sync for WeakDarc<T> {}

impl<T> WeakDarc<T> {
    /// attempts to upgrade the `WeakDarc` to a [Darc], if the inner value has not been dropped
    /// returns `None` if the value has been dropped
    pub fn upgrade(&self) -> Option<Darc<T>> {
        let inner = &*self.inner;
        inner.local_cnt.fetch_add(1, Ordering::SeqCst);
        let id = inner.total_local_cnt.fetch_add(1, Ordering::SeqCst);
        if inner.valid.load(Ordering::SeqCst) {
            Some(Darc {
                inner: self.inner.clone(),
                src_pe: self.src_pe,
                id,
            })
        } else {
            let cnt = inner.local_cnt.fetch_sub(1, Ordering::SeqCst);
            if cnt == 0 {
                panic!("darc dropped too many times");
            }
            None
        }
    }
}

impl<T> Drop for WeakDarc<T> {
    fn drop(&mut self) {
        let inner = &*self.inner;
        // println!("dropping weak darc\n {:?}", inner);
        inner.weak_local_cnt.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<T> Clone for WeakDarc<T> {
    fn clone(&self) -> Self {
        let inner = &*self.inner;
        inner.weak_local_cnt.fetch_add(1, Ordering::SeqCst);
        WeakDarc {
            inner: self.inner.clone(),
            src_pe: self.src_pe,
        }
    }
}

impl<T> crate::active_messaging::DarcSerde for Darc<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>) {
        trace!("darc ser {:?} ", self.inner());
        // println!("darc ser");
        self.serialize_update_cnts(num_pes);
        trace!("darc ser {:?} ", self.inner());
        darcs.push(RemotePtr::NetworkDarc(self.clone().into()));
        // self.print();
    }

    // this occurs in the From<NetworkDarc> for Darc impl so we should be able to delete this...
    // #[tracing::instrument(skip_all, level = "debug")]
    // fn des(&self, cur_pe: Result<usize, IdError>) {
    // trace!("darc des");
    // match cur_pe {
    //     Ok(_) => {
    //         self.deserialize_update_cnts();
    //     }
    //     Err(err) => {
    //         panic!("can only access darcs within team members ({:?})", err);
    //     }
    // }
    // self.print();
    // }
}

impl<T: 'static> DarcInner<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        unsafe {
            Arc::increment_strong_count(self.team);
            Pin::new_unchecked(Arc::from_raw(self.team))
        }
    }

    fn am_counters(&self) -> Arc<AMCounters> {
        unsafe {
            Arc::increment_strong_count(self.am_counters);
            Arc::from_raw(self.am_counters)
        }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn inc_pe_ref_count(&self, pe: usize, amt: usize) -> usize {
        // if self.ref_cnt_addr + pe * std::mem::size_of::<AtomicUsize>() < 10 {
        // if self.ref_cnt_slice.index_addr(pe) < 10 {
        //     println!("error!!!! addrress makes no sense: {:?} ", pe);
        //     println!("{:?}", self);
        //     panic!();
        // }
        trace!("inc_pe_ref_count pe: {} amt: {} {:?}", pe, amt, self);
        let team_pe = pe;
        let tot_ref_cnt = unsafe {
            // ((self.total_ref_cnt_addr + team_pe * std::mem::size_of::<AtomicUsize>())
            //     as *mut AtomicUsize)
            //     .as_ref()
            //     .expect("invalid darc addr")
            (&self.total_ref_cnt_slice[team_pe] as *const _ as *const AtomicUsize)
                .as_ref()
                .expect("invalid darc addr")
        };
        tot_ref_cnt.fetch_add(amt, Ordering::SeqCst);
        let ref_cnt = unsafe {
            (&self.ref_cnt_slice[team_pe] as *const _ as *const AtomicUsize)
                .as_ref()
                .expect("invalid darc addr")
        };
        ref_cnt.fetch_add(amt, Ordering::SeqCst)
    }

    // async fn barrier(&self) -> &B{
    //     let barrier_fut = unsafe { (*self.barrier).async_barrier() };
    //     barrier_fut.await;
    // }

    fn update_item(&mut self, item: *const T) {
        self.item = item;
    }

    #[allow(dead_code)]
    fn item(&self) -> &T {
        unsafe { &(*self.item) }
    }

    #[tracing::instrument(skip_all, level = "debug")]
    fn send_finished(&self) -> Vec<LamellarTask<()>> {
        let ref_cnts = unsafe {
            self.ref_cnt_slice
                .as_casted_slice::<AtomicUsize>()
                .expect("invalid ref cnt slice")
        };
        let team = self.team();
        let mut reqs = vec![];
        for pe in 0..ref_cnts.len() {
            let cnt = ref_cnts[pe].swap(0, Ordering::SeqCst);

            if cnt > 0 {
                let my_addr = &*self as *const DarcInner<T> as usize;
                let pe_addr = team.lamellae.comm().remote_addr(
                    team.arch.world_pe(pe).expect("invalid team member"),
                    my_addr,
                );
                debug!(
                    "[{:?}] sending finished to {:?} {:?} team {:?} {:x}",
                    std::thread::current().id(),
                    pe,
                    cnt,
                    team.team_hash,
                    my_addr
                );
                // println!("[{:?}] {:?}", std::thread::current().id(), self);
                reqs.push(
                    team.spawn_am_pe_tg(
                        pe,
                        FinishedAm {
                            cnt: cnt,
                            src_pe: pe,
                            inner_addr: pe_addr,
                        },
                        Some(self.am_counters()),
                    )
                    .spawn(),
                );
            }
        }
        reqs
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn wait_on_state(
        // inner: WrappedInner<T>,
        inner: DarcCommPtr<T>,
        state: DarcMode,
        extra_cnt: usize,
        reset: bool,
    ) -> bool {
        let team = inner.team();
        let rdma = team.lamellae.comm();
        rdma.flush();
        for pe in inner.mode_slice.iter() {
            let timer = std::time::Instant::now();
            while *pe != state {
                if inner.local_cnt.load(Ordering::SeqCst) == 1 + extra_cnt {
                    join_all(inner.send_finished()).await;
                }
                if !reset && timer.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                    // let ref_cnts_slice = unsafe {
                    //     std::slice::from_raw_parts_mut(
                    //         inner.ref_cnt_addr as *mut usize,
                    //         inner.num_pes,
                    //     )
                    // };
                    println!("[{:?}][{:?}][WARNING] -- Potential deadlock detected.\n\
                        The runtime is currently waiting for all remaining references to this distributed object to be dropped.\n\
                        The object is likely a {:?} with {:?} remaining local references and {:?} remaining remote references, ref cnts by pe {:?}\n\
                        An example where this can occur can be found at https://docs.rs/lamellar/latest/lamellar/array/struct.ReadOnlyArray.html#method.into_local_lock\n\
                        The deadlock timeout can be set via the LAMELLAR_DEADLOCK_WARNING_TIMEOUT environment variable, the current timeout is {} seconds\n\
                        To view backtrace set RUST_LIB_BACKTRACE=1\n\
                        {}",
                        inner.my_pe,
                        std::thread::current().id(),
                        inner.mode_slice.as_slice(),
                        inner.local_cnt.load(Ordering::SeqCst),
                        inner.dist_cnt.load(Ordering::SeqCst),
                        inner.ref_cnt_slice,
                        config().deadlock_warning_timeout,
                        std::backtrace::Backtrace::capture()
                    );
                }
                if reset && timer.elapsed().as_secs_f64() > config().deadlock_warning_timeout / 2.0
                {
                    println!("[{:?}][{:?}][WARNING] -- Sending RestartDrop.\n\
                        The runtime is currently waiting for all remaining references to this distributed object to be dropped. The object is likely a {:?}\n\
                         with {:?} remaining local references and {:?} remaining remote references, ref cnts by pe {:?}\n\
                        To view backtrace set RUST_LIB_BACKTRACE=1\n\
                        {}",
                        inner.my_pe,
                        std::thread::current().id(),
                        inner.mode_slice.as_slice(),
                        inner.local_cnt.load(Ordering::SeqCst),
                        inner.dist_cnt.load(Ordering::SeqCst),
                        inner.ref_cnt_slice,
                        std::backtrace::Backtrace::capture()
                    );
                    return false;
                }
                if reset && inner.mode_slice.iter().any(|x| *x == DarcMode::RestartDrop) {
                    return false;
                }
                rdma.flush();
                async_std::task::yield_now().await;
            }
        }
        true
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn broadcast_state(
        // mut inner: WrappedInner<T>,
        inner: DarcCommPtr<T>,
        team: Pin<Arc<LamellarTeamRT>>,
        state: DarcMode,
    ) {
        // unsafe {
        //     (*(((&inner.mode_slice[inner.my_pe]) as *const DarcMode) as *const AtomicU64)) //this should be fine given that DarcMode uses Repr(u64)
        //         .store(state as u64, Ordering::SeqCst)
        // };
        let rdma = team.lamellae.comm();
        // let mut txs = Vec::new();
        let my_pe = inner.my_pe;
        trace!(
            "broadcast state {:?} {:?} {:?}",
            state,
            inner.mode_slice.as_ptr(),
            inner.mode_slice.index_addr(my_pe)
        );
        // let mut reqs = vec![];
        for pe in team.arch.team_iter() {
            trace!("putting state {:?} to pe {}", state, pe);
            // reqs.push(
            inner.mode_slice.put_unmanaged(
                // &team.scheduler,
                // team.counters(),
                state, //inner.mode_slice.sub_slice(my_pe..=my_pe),
                pe, my_pe,
            );
            //);
        }
        // join_all(reqs).await;
        rdma.wait();
        trace!("broadcasted state {:?}", state);
        // join_all(txs).await;
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn block_on_outstanding(
        // mut inner: WrappedInner<T>
        mut inner: DarcCommPtr<T>,
        state: DarcMode,
        extra_cnt: usize,
    ) {
        let team = inner.team();
        // let mode_refs_slice = inner.mode_slice;
        // let mode_refs =
        //     unsafe { std::slice::from_raw_parts_mut(inner.mode_addr as *mut u8, inner.num_pes) };
        let orig_state = inner.mode_slice[inner.my_pe];
        trace!(
            "[{:?}] entering block_on_outstanding {:?} {:?}",
            std::thread::current().id(),
            inner.as_ptr(),
            inner.mode_slice.as_ptr(),
        );
        inner.await_all().await;
        if team.num_pes() == 1 {
            while inner.local_cnt.load(Ordering::SeqCst) > 1 + extra_cnt {
                async_std::task::yield_now().await;
            }
            unsafe {
                (*(((&inner.mode_slice[inner.my_pe]) as *const DarcMode) as *const AtomicU64)) //this should be fine given that DarcMode uses Repr(u64)
                    .store(state as u64, Ordering::SeqCst)
            };
        } else {
            let mut outstanding_refs = true;

            let mut prev_ref_cnts = vec![0usize; inner.num_pes];
            let mut barrier_id = 1usize;

            while inner.local_cnt.load(Ordering::SeqCst) > 1 + extra_cnt {
                async_std::task::yield_now().await;
            }
            join_all(inner.send_finished()).await;

            trace!(
                "[{:?}] entering initial block_on barrier() {:?}",
                std::thread::current().id(),
                inner.as_ref()
            );
            if !Self::wait_on_state(inner.clone(), orig_state, extra_cnt, false).await {
                panic!("deadlock waiting for original state");
            }
            let barrier_fut = unsafe { inner.barrier.as_ref().unwrap().async_barrier() };
            barrier_fut.await;
            trace!(
                "[{:?}] leaving initial block_on barrier() {:?}",
                std::thread::current().id(),
                inner.as_ref()
            );

            while outstanding_refs {
                if inner.mode_slice.iter().any(|x| *x == DarcMode::RestartDrop) {
                    Self::broadcast_state(inner.clone(), team.clone(), DarcMode::RestartDrop).await;
                    if !(Self::wait_on_state(
                        inner.clone(),
                        DarcMode::RestartDrop,
                        extra_cnt,
                        false,
                    )
                    .await)
                    {
                        panic!("deadlock");
                    }
                    Self::broadcast_state(inner.clone(), team.clone(), orig_state).await;
                    // team.scheduler.submit_task(async move {
                    Box::pin(DarcInner::block_on_outstanding(
                        inner.clone(),
                        state,
                        extra_cnt,
                    ))
                    .await;
                    // });
                    return;
                }
                trace!(
                    "[{:?}] starting block_on loop iteration barrier_id: {:?} {:?}",
                    std::thread::current().id(),
                    barrier_id,
                    inner.as_ref()
                );
                outstanding_refs = false;
                // these hopefully all get set to non zero later otherwise we still need to wait
                for id in inner.mode_barrier_slice.iter_mut() {
                    *id = 0;
                }
                let old_barrier_id = barrier_id; //we potentially will set barrier_id to 0 but want to maintiain the previously highest value
                while inner.local_cnt.load(Ordering::SeqCst) > 1 + extra_cnt {
                    async_std::task::yield_now().await;
                }
                join_all(inner.send_finished()).await;
                let barrier_fut = unsafe { inner.barrier.as_ref().unwrap().async_barrier() };
                barrier_fut.await;
                trace!(
                    "[{:?}]  finished initial barrier {:?}",
                    std::thread::current().id(),
                    inner.as_ref()
                );

                let old_ref_cnts = inner.total_ref_cnt_slice.to_vec();
                let old_local_cnt = inner.total_local_cnt.load(Ordering::SeqCst);
                let old_dist_cnt = inner.total_dist_cnt.load(Ordering::SeqCst);

                let rdma = team.lamellae.comm();
                // let mut dist_cnts_changed = false;
                for pe in 0..inner.num_pes {
                    // let ref_cnt_u8 = unsafe {
                    //     std::slice::from_raw_parts_mut(
                    //         &mut old_ref_cnts[pe] as *mut usize as *mut u8,
                    //         std::mem::size_of::<usize>(),
                    //     )
                    // };

                    if prev_ref_cnts[pe] != old_ref_cnts[pe] {
                        let send_pe = team.arch.single_iter(pe).next().unwrap();

                        // rdma.put(
                        //     &team.scheduler,
                        //     team.counters(),
                        //     send_pe,
                        //     // poor mans way of essentially doing a "scoped" async task, we guarantee
                        //     // the ref_cnt underlying data is valid since we will await the put immediately
                        //     // (before old_ref_cnts is dropped)
                        //     unsafe { CommSlice::from_raw_parts(&mut old_ref_cnts[pe], 1) },
                        //     inner.mode_ref_cnt_slice.index_addr(inner.my_pe), //this is barrier_ref_cnt_slice
                        // )
                        // .await;
                        trace!(
                            "[{:?}] putting ref cnt {:?} to pe {:?} {:?}",
                            std::thread::current().id(),
                            old_ref_cnts[pe],
                            send_pe,
                            inner.as_ref()
                        );
                        inner.mode_ref_cnt_slice.put_unmanaged(
                            // &team.scheduler,
                            // team.counters(),
                            // poor mans way of essentially doing a "scoped" async task, we guarantee
                            // the ref_cnt underlying data is valid since we will await the put immediately
                            // (before old_ref_cnts is dropped)
                            old_ref_cnts[pe], //unsafe { CommSlice::from_raw_parts(&mut old_ref_cnts[pe], 1) },
                            send_pe,
                            inner.my_pe,
                        );
                        // .await;
                        outstanding_refs = true;
                        barrier_id = 0;
                    }
                }
                rdma.wait();
                trace!(
                    "[{:?}]  finished putting ref cnts {:?}",
                    std::thread::current().id(),
                    inner.as_ref()
                );
                rdma.flush();
                let barrier_fut = unsafe { inner.barrier.as_ref().unwrap().async_barrier() };
                barrier_fut.await;
                trace!(
                    "[{:?}]  finished barrier after putting ref cnts {:?}",
                    std::thread::current().id(),
                    inner.as_ref()
                );
                outstanding_refs |= old_local_cnt != inner.total_local_cnt.load(Ordering::SeqCst);
                outstanding_refs |= old_dist_cnt != inner.total_dist_cnt.load(Ordering::SeqCst);

                let mut barrier_sum = 0;
                for pe in 0..inner.num_pes {
                    outstanding_refs |= old_ref_cnts[pe] != inner.total_ref_cnt_slice[pe];
                    barrier_sum += inner.mode_ref_cnt_slice[pe];
                }
                outstanding_refs |= barrier_sum != old_dist_cnt;

                if outstanding_refs {
                    barrier_id = 0;
                }
                rdma.flush();
                let barrier_fut = unsafe { inner.barrier.as_ref().unwrap().async_barrier() };
                barrier_fut.await;
                trace!(
                    "[{:?}]  finished barrier after checking ref cnts {:?}",
                    std::thread::current().id(),
                    inner.as_ref()
                );

                // let barrier_id_slice = unsafe {
                //     std::slice::from_raw_parts_mut(
                //         &mut barrier_id as *mut usize as *mut u8,
                //         std::mem::size_of::<usize>(),
                //     )
                // };

                for pe in 0..inner.num_pes {
                    let send_pe = team.arch.single_iter(pe).next().unwrap();

                    // rdma.put(
                    //     &team.scheduler,
                    //     team.counters(),
                    //     send_pe,
                    //     // barrier_id_slice,
                    //     // poor mans way of essentially doing a "scoped" async task, we guarantee
                    //     // the barrier_id underlying data is valid since we will await the put immediately
                    //     // (before old_ref_cnts is dropped)
                    //     unsafe { CommSlice::from_raw_parts(&mut barrier_id, 1) },
                    //     // inner.mode_barrier_addr + inner.my_pe * std::mem::size_of::<usize>(),
                    //     inner.mode_barrier_slice.index_addr(inner.my_pe),
                    // )
                    // .await;

                    inner.mode_barrier_slice.put_unmanaged(
                        // &team.scheduler,
                        // team.counters(),
                        // poor mans way of essentially doing a "scoped" async task, we guarantee
                        // the barrier_id underlying data is valid since we will await the put immediately
                        // (before old_ref_cnts is dropped)
                        barrier_id, // unsafe { CommSlice::from_raw_parts(&mut barrier_id, 1) },
                        send_pe,
                        inner.my_pe,
                    );
                    // .await;
                }
                rdma.wait();
                trace!(
                    "[{:?}]  after putting mode_barrier_slice{:?}",
                    std::thread::current().id(),
                    inner.as_ref()
                );
                //maybe we need to change the above to a get?
                rdma.flush();

                let barrier_fut = unsafe { inner.barrier.as_ref().unwrap().async_barrier() };
                barrier_fut.await;
                trace!(
                    "[{:?}]  after barrier after putting mode_barrier_slice{:?}",
                    std::thread::current().id(),
                    inner.as_ref()
                );

                for id in inner.mode_barrier_slice.iter_mut() {
                    outstanding_refs |= *id == 0;
                }

                barrier_id = old_barrier_id + 1;
                prev_ref_cnts = old_ref_cnts;
                let barrier_fut = unsafe { inner.barrier.as_ref().unwrap().async_barrier() };
                barrier_fut.await;
                trace!(
                    "[{:?}]  end of block_on loop iteration barrier_id: {:?} outstanding_refs: {:?} {:?}",
                    std::thread::current().id(),
                    barrier_id,
                    outstanding_refs,
                    inner.as_ref()
                );
            }
            trace!(
                "[{:?}]  all outstanding refs are resolved {:?}",
                std::thread::current().id(),
                inner.as_ref(),
            );
            Self::broadcast_state(inner.clone(), team.clone(), state).await;
            let barrier_fut = unsafe { inner.barrier.as_ref().unwrap().async_barrier() };
            barrier_fut.await;
            trace!(
                "[{:?}]  after barrier after putting dropped{:?}",
                std::thread::current().id(),
                inner.mode_slice.as_slice()
            );
            if !Self::wait_on_state(inner.clone(), state, extra_cnt, true).await {
                Self::broadcast_state(inner.clone(), team.clone(), DarcMode::RestartDrop).await;
                if !(Self::wait_on_state(inner.clone(), DarcMode::RestartDrop, extra_cnt, false)
                    .await)
                {
                    panic!("deadlock");
                }
                Self::broadcast_state(inner.clone(), team.clone(), orig_state).await;
                // team.scheduler.submit_task(async move {
                Box::pin(DarcInner::block_on_outstanding(
                    inner.clone(),
                    state,
                    extra_cnt,
                ))
                .await;
                // });
                return;
            }

            // self.debug_print();
            // println!("{rel_addr:x}  {:?}", self);
            let barrier_fut = unsafe { inner.barrier.as_ref().unwrap().async_barrier() };
            barrier_fut.await;
        }

        // self.debug_print();
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn await_all(&self) {
        self.team().lamellae.comm().wait();
        let mut temp_now = Instant::now();
        let am_counters = self.am_counters();
        let mut orig_reqs = am_counters.send_req_cnt.load(Ordering::SeqCst);
        let mut orig_launched = am_counters.launched_req_cnt.load(Ordering::SeqCst);
        let mut done = false;
        while !done {
            while self.team().panic.load(Ordering::SeqCst) == 0
                && ((am_counters.outstanding_reqs.load(Ordering::SeqCst) > 0)
                    || orig_reqs != am_counters.send_req_cnt.load(Ordering::SeqCst)
                    || orig_launched != am_counters.launched_req_cnt.load(Ordering::SeqCst))
            {
                orig_reqs = am_counters.send_req_cnt.load(Ordering::SeqCst);
                orig_launched = am_counters.launched_req_cnt.load(Ordering::SeqCst);
                async_std::task::yield_now().await;
                if temp_now.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                    println!(
                        "in darc await_all mype: {:?} cnt: {:?} {:?}",
                        self.team().world_pe,
                        am_counters.send_req_cnt.load(Ordering::SeqCst),
                        am_counters.outstanding_reqs.load(Ordering::SeqCst),
                    );
                    temp_now = Instant::now();
                }
            }
            if am_counters.send_req_cnt.load(Ordering::SeqCst)
                != am_counters.launched_req_cnt.load(Ordering::SeqCst)
            {
                if am_counters.outstanding_reqs.load(Ordering::SeqCst) > 0
                    || orig_reqs != am_counters.send_req_cnt.load(Ordering::SeqCst)
                    || orig_launched != am_counters.launched_req_cnt.load(Ordering::SeqCst)
                {
                    continue;
                }
                println!(
                    "in darc await_all mype: {:?} cnt: {:?} {:?} {:?}",
                    self.team().world_pe,
                    am_counters.send_req_cnt.load(Ordering::SeqCst),
                    am_counters.outstanding_reqs.load(Ordering::SeqCst),
                    am_counters.launched_req_cnt.load(Ordering::SeqCst)
                );
                RuntimeWarning::UnspawnedTask(
                    "`await_all` before all tasks/active messages have been spawned",
                )
                .print();
            }
            done = true;
        }
    }
}

impl<T: 'static> fmt::Debug for DarcInner<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // write!(
        //     f,
        //     "[{:}/{:?}] lc: {:?} dc: {:?} wc: {:?} ref_cnt: {:?} am_cnt ({:?},{:?}) mode {:?}",
        //     self.my_pe,
        //     self.num_pes,
        //     self.local_cnt.load(Ordering::SeqCst),
        //     self.dist_cnt.load(Ordering::SeqCst),
        //     self.weak_local_cnt.load(Ordering::SeqCst),
        //     self.ref_cnt_slice.as_slice(),
        //     self.am_counters().outstanding_reqs.load(Ordering::Relaxed),
        //     self.am_counters().send_req_cnt.load(Ordering::Relaxed),
        //     self.mode_slice.as_slice(),
        // )
        write!(f, "[{:}/{:?}] ", self.my_pe, self.num_pes)?;
        write!(f, "lc: {:?} ", self.local_cnt.load(Ordering::SeqCst))?;
        write!(f, "dc: {:?} ", self.dist_cnt.load(Ordering::SeqCst))?;
        write!(f, "wc: {:?} ", self.weak_local_cnt.load(Ordering::SeqCst))?;
        write!(f, "ref_cnt: {:?} ", self.ref_cnt_slice.as_slice())?;
        write!(
            f,
            "am_cnt ({:?},{:?}) ",
            self.am_counters().outstanding_reqs.load(Ordering::Relaxed),
            self.am_counters().send_req_cnt.load(Ordering::Relaxed)
        )?;
        write!(f, "mode {:?} ", self.mode_slice.as_ptr())
    }
}

impl<T> Darc<T> {
    //#[doc(hidden)]
    /// downgrade a darc to a weak darc
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn downgrade(the_darc: &Darc<T>) -> WeakDarc<T> {
        debug!("downgrading darc {:?}", the_darc.id);
        // the_darc.print();
        the_darc
            .inner()
            .weak_local_cnt
            .fetch_add(1, Ordering::SeqCst);

        let weak = WeakDarc {
            inner: the_darc.inner.clone(),
            src_pe: the_darc.src_pe,
        };
        // the_darc.print();
        weak
    }

    pub(crate) fn inner(&self) -> &DarcInner<T> {
        self.inner.as_ref().expect("invalid darc inner ptr")
    }
    fn inner_mut(&self) -> &mut DarcInner<T> {
        self.inner.as_mut().expect("invalid darc inner ptr")
    }
    #[allow(dead_code)]
    pub(crate) fn team(&self) -> Pin<Arc<LamellarTeamRT>> {
        self.inner().team()
    }
    // fn ref_cnts_as_mut_slice(&self) -> &mut [usize] {
    //     self.inner().ref_cnt_slice.as_mut_slice()
    //     // unsafe { std::slice::from_raw_parts_mut(inner.ref_cnt_addr as *mut usize, inner.num_pes) }
    // }
    // fn mode_as_mut_slice(&self) -> &mut [DarcMode] {
    //     self.inner().mode_slice.as_mut_slice()
    //     // let inner = self.inner();
    //     // unsafe { std::slice::from_raw_parts_mut(inner.mode_addr as *mut DarcMode, inner.num_pes) }
    // }
    // fn mode_barrier_as_mut_slice(&self) -> &mut [usize] {
    //     self.inner().mode_barrier_slice.as_mut_slice()
    //     // let inner = self.inner();
    //     // unsafe {
    //     //     std::slice::from_raw_parts_mut(inner.mode_barrier_addr as *mut usize, inner.num_pes)
    //     // }
    // }
    // fn mode_ref_cnt_as_mut_slice(&self) -> &mut [usize] {
    //     self.inner().mode_ref_cnt_slice.as_mut_slice()
    //     // let inner = self.inner();
    //     // unsafe {
    //     //     std::slice::from_raw_parts_mut(inner.mode_ref_cnt_addr as *mut usize, inner.num_pes)
    //     // }
    // }

    #[doc(hidden)]
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn serialize_update_cnts(&self, cnt: usize) {
        trace!("darc[{:?}] serialize darc cnts {:?}", self.id, self.inner());
        self.inner()
            .dist_cnt
            .fetch_add(cnt, std::sync::atomic::Ordering::SeqCst);
        self.inner()
            .total_dist_cnt
            .fetch_add(cnt, std::sync::atomic::Ordering::SeqCst);
        // self.print();
        // println!("done serialize darc cnts");
    }

    // this occurs in the From<NetworkDarc> for Darc impl so we should be able to delete this...
    #[doc(hidden)]
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn deserialize_update_cnts(&self) {
        // println!("deserialize darc? cnts");
        trace!(
            "darc[{:?}] deserialize darc cnts {:?}",
            self.id,
            self.inner()
        );
        self.inner().inc_pe_ref_count(self.src_pe, 1);
        self.inner().local_cnt.fetch_add(1, Ordering::SeqCst);
        self.inner().total_local_cnt.fetch_add(1, Ordering::SeqCst);
        // println! {"[{:?}] darc[{:?}] deserialized {:?} {:?} {:?}",std::thread::current().id(),self.inner().id,self.inner,self.inner().local_cnt.load(Ordering::SeqCst), self.inner().total_local_cnt.load(Ordering::SeqCst)};
        // self.print();
        // println!("done deserialize darc cnts");
    }

    #[doc(hidden)]
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn inc_local_cnt(&self, cnt: usize) -> usize {
        trace!("darc[{:?}] inc_local_cnt {:?}", self.id, self.inner());
        self.inner().local_cnt.fetch_add(cnt, Ordering::SeqCst);
        self.inner()
            .total_local_cnt
            .fetch_add(cnt, Ordering::SeqCst)
        // println!(
        //     "[{:?}] darc[{:?}] inc_local_cnt {:?} {:?}",
        //     std::thread::current().id(),
        //     self.inner().id,
        //     self.inner().local_cnt.load(Ordering::SeqCst),
        //     self.inner().total_local_cnt.load(Ordering::SeqCst)
        // );
    }

    #[doc(hidden)]
    pub fn print(&self) {
        println!(
            "[{:?}]--------\nid: {:?}:{:?} orig: {:?} 0x{:x} item_addr {:?} {:?}\n--------[{:?}]",
            std::thread::current().id(),
            self.inner().id,
            self.id,
            self.src_pe,
            self.inner.addr(),
            self.inner().item,
            self.inner(),
            std::thread::current().id(),
        );
    }
}

fn calc_padding(addr: usize, align: usize) -> usize {
    let rem = addr % align;
    if rem == 0 {
        0
    } else {
        align - rem
    }
}

impl<T: Send + Sync> Darc<T> {
    #[doc(alias = "Collective")]
    /// Constructs a new `Darc<T>` on the PEs specified by team.
    ///
    /// This is a blocking collective call amongst all PEs in the team, only returning once every PE in the team has completed the call.
    ///
    /// Returns an error if this PE is not a part of team
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `team` to enter the constructor call otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    ///
    /// ```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let five = Darc::new(&world,5).block().expect("PE in world team");
    /// ```
    #[tracing::instrument(skip_all, level = "debug")]
    pub fn new<U: Into<IntoLamellarTeam>>(team: U, item: T) -> DarcHandle<T> {
        let team = team.into().team.clone();
        DarcHandle {
            team: team.clone(),
            launched: false,
            creation_future: Box::pin(Darc::async_try_new_with_drop(
                team,
                item,
                DarcMode::Darc,
                None,
            )),
        }
    }
    // pub fn new<U: Into<IntoLamellarTeam>>(team: U, item: T) -> Result<Darc<T>, IdError> {
    //     Darc::try_new_with_drop(team, item, DarcMode::Darc, None)
    // }

    // pub(crate) async fn async_try_new<U: Into<IntoLamellarTeam>>(
    //     team: U,
    //     item: T,
    //     state: DarcMode,
    // ) -> Result<Darc<T>, IdError> {
    //     Darc::async_try_new_with_drop(team, item, state, None).await
    // }

    // pub(crate) fn try_new<U: Into<IntoLamellarTeam>>(
    //     team: U,
    //     item: T,
    //     state: DarcMode,
    // ) -> Result<Darc<T>, IdError> {
    //     Darc::try_new_with_drop(team, item, state, None)
    // }

    #[tracing::instrument(skip_all, level = "debug")]
    pub(crate) async fn async_try_new_with_drop<U: Into<IntoLamellarTeam>>(
        team: U,
        item: T,
        state: DarcMode,
        drop: Option<fn(&mut T) -> bool>,
    ) -> Result<Darc<T>, IdError> {
        let team_rt = team.into().team.clone();
        let my_pe = team_rt.team_pe?;

        let alloc = if team_rt.num_pes == team_rt.num_world_pes {
            AllocationType::Global
        } else {
            AllocationType::Sub(team_rt.get_pes())
        };

        //The DarcInner data structure
        let mut size = std::mem::size_of::<DarcInner<T>>();

        // Ref Cnt Array
        let padding = calc_padding(size, std::mem::align_of::<usize>());
        let ref_cnt_offset = size + padding;
        size += padding + team_rt.num_pes * std::mem::size_of::<usize>();

        // total ref cnt array
        let padding = calc_padding(size, std::mem::align_of::<usize>());
        let total_ref_cnt_offset = size + padding;
        size += padding + team_rt.num_pes * std::mem::size_of::<usize>();

        // mode array
        let padding = calc_padding(size, std::mem::align_of::<DarcMode>());
        let mode_offset = size + padding;
        size += padding + team_rt.num_pes * std::mem::size_of::<DarcMode>();

        //mode ref cnt array
        let padding = calc_padding(size, std::mem::align_of::<usize>());
        let mode_ref_cnt_offset = size + padding;
        size += padding + team_rt.num_pes * std::mem::size_of::<usize>();

        //mode_barrier array
        let padding = calc_padding(size, std::mem::align_of::<usize>());
        let mode_barrier_offset = size + padding;
        size += padding + team_rt.num_pes * std::mem::size_of::<usize>();

        // println!("creating new darc");

        team_rt.async_barrier().await;
        // println!("creating new darc after barrier");
        let darc_alloc = team_rt
            .lamellae
            .comm()
            .alloc(size, alloc, std::mem::align_of::<DarcInner<T>>())
            .expect("out of memory");
        trace!(
            "[{:?}] creating new darc[{:?}] {:?} alloc: {:?}",
            std::thread::current().id(),
            DARC_ID.load(Ordering::Relaxed),
            team_rt.team_hash,
            darc_alloc
        );
        // let temp_team = team_rt.clone();
        // team_rt.print_cnt();
        let team_ptr = unsafe {
            let pinned_team = Pin::into_inner_unchecked(team_rt.clone());
            Arc::into_raw(pinned_team)
        };
        // team_rt.print_cnt();
        let am_counters = Arc::new(AMCounters::new());
        let am_counters_ptr = Arc::into_raw(am_counters);
        let barrier = Box::new(Barrier::new(
            team_rt.world_pe,
            team_rt.num_world_pes,
            team_rt.lamellae.clone(),
            team_rt.arch.clone(),
            team_rt.scheduler.clone(),
            team_rt.panic.clone(),
        ));
        let barrier_ptr = Box::into_raw(barrier);
        unsafe {
            let darc_temp_ptr = darc_alloc.as_mut_ptr::<DarcInner<T>>();
            // let darc_temp = DarcInner {
            (*darc_temp_ptr).id = DARC_ID.fetch_add(1, Ordering::Relaxed);
            (*darc_temp_ptr).my_pe = my_pe;
            (*darc_temp_ptr).num_pes = team_rt.num_pes;
            (*darc_temp_ptr).local_cnt = AtomicUsize::new(1);
            (*darc_temp_ptr).total_local_cnt = AtomicUsize::new(1);
            (*darc_temp_ptr).weak_local_cnt = AtomicUsize::new(0);
            (*darc_temp_ptr).dist_cnt = AtomicUsize::new(0);
            (*darc_temp_ptr).total_dist_cnt = AtomicUsize::new(0);
            (*darc_temp_ptr).ref_cnt_slice =
                darc_alloc.comm_slice_at_byte_offset(ref_cnt_offset, team_rt.num_pes);
            trace!(
                "ref cnt slice {:?} padding: {:?}",
                (*darc_temp_ptr).ref_cnt_slice,
                calc_padding(
                    (*darc_temp_ptr).ref_cnt_slice.usize_addr(),
                    std::mem::align_of::<usize>()
                )
            );
            (*darc_temp_ptr).total_ref_cnt_slice =
                darc_alloc.comm_slice_at_byte_offset(total_ref_cnt_offset, team_rt.num_pes);
            trace!(
                "total ref cnt slice {:?} padding: {:?}",
                (*darc_temp_ptr).total_ref_cnt_slice,
                calc_padding(
                    (*darc_temp_ptr).total_ref_cnt_slice.usize_addr(),
                    std::mem::align_of::<usize>()
                )
            );
            (*darc_temp_ptr).mode_slice =
                darc_alloc.comm_slice_at_byte_offset(mode_offset, team_rt.num_pes);
            trace!(
                "mode slice {:?} padding: {:?}",
                (*darc_temp_ptr).mode_slice,
                calc_padding(
                    (*darc_temp_ptr).mode_slice.usize_addr(),
                    std::mem::align_of::<u64>()
                )
            );
            (*darc_temp_ptr).mode_ref_cnt_slice =
                darc_alloc.comm_slice_at_byte_offset(mode_ref_cnt_offset, team_rt.num_pes);
            trace!(
                "mode ref cnt slice {:?} padding: {:?}",
                (*darc_temp_ptr).mode_ref_cnt_slice,
                calc_padding(
                    (*darc_temp_ptr).mode_ref_cnt_slice.usize_addr(),
                    std::mem::align_of::<usize>()
                )
            );
            (*darc_temp_ptr).mode_barrier_slice =
                darc_alloc.comm_slice_at_byte_offset(mode_barrier_offset, team_rt.num_pes);
            trace!(
                "mode barrier slice {:?} padding: {:?}",
                (*darc_temp_ptr).mode_barrier_slice,
                calc_padding(
                    (*darc_temp_ptr).mode_barrier_slice.usize_addr(),
                    std::mem::align_of::<usize>()
                )
            );
            (*darc_temp_ptr).barrier = barrier_ptr;
            // mode_barrier_rounds: num_rounds,
            (*darc_temp_ptr).am_counters = am_counters_ptr;
            (*darc_temp_ptr).team = team_ptr; //&team_rt, //Arc::into_raw(temp_team),
            (*darc_temp_ptr).item = Box::into_raw(Box::new(item));
            (*darc_temp_ptr).drop = drop;
            (*darc_temp_ptr).valid = AtomicBool::new(true);
        }
        // };
        // unsafe {
        //     std::ptr::copy_nonoverlapping(&darc_temp, darc_alloc.as_mut_ptr::<DarcInner<T>>(), 1);
        // }
        // std::mem::forget(darc_temp); // prevent double free because CommSlices may contain Arc<LibfabricAlloc>s, which would go down to a count of 0 if darc_temp is dropped
        // println!("Darc Inner Item Addr: {:?}", darc_temp.item);

        let d = Darc {
            inner: DarcCommPtr {
                alloc: darc_alloc,
                _phantom: std::marker::PhantomData::<DarcInner<T>>,
            }, //.addr as *mut DarcInner<T>,
            src_pe: my_pe,
            id: 0,
        };
        for elem in d.inner().ref_cnt_slice.clone().iter_mut() {
            *elem = 0;
        }
        for elem in d.inner().total_ref_cnt_slice.clone().iter_mut() {
            *elem = 0;
        }
        for elem in d.inner().mode_slice.clone().iter_mut() {
            *elem = state;
        }
        for elem in d.inner().mode_ref_cnt_slice.clone().iter_mut() {
            *elem = 0;
        }
        for elem in d.inner().mode_barrier_slice.clone().iter_mut() {
            *elem = 0;
        }
        trace!(
            " [{:?}] created new darc[{:?}] , next_inner_id: {:?} {:?} ",
            std::thread::current().id(),
            d.id,
            DARC_ID.load(Ordering::Relaxed),
            d.inner(),
        );
        // d.print();
        team_rt.async_barrier().await;
        // team_rt.print_cnt();
        Ok(d)
    }

    pub(crate) async fn block_on_outstanding(self, state: DarcMode, extra_cnt: usize) {
        // let wrapped = WrappedInner {
        //     inner: NonNull::new(unsafe{self.inner.as_mut_ptr()}).expect("invalid darc pointer"),
        // };
        // let inner = self.darc.inner.clone();
        DarcInner::block_on_outstanding(self.inner.clone(), state, extra_cnt).await;
    }

    #[doc(alias = "Collective")]
    /// Converts this Darc into a [LocalRwDarc]
    ///
    /// This returns a handle (which is Future) thats needs to be `awaited` or `blocked` on to perform the operation.
    /// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Darc's team, only returning once every PE in the team has completed the call.
    ///
    /// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
    /// pointed to object to wrapped by both a Darc and a LocalRwDarc simultaneously (on any PE).
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    /// ```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let five_handle = Darc::new(&world,5);
    /// let five_as_localdarc = world.block_on(async move {
    ///     let five = five_handle.await.expect("PE in world team");
    ///     five.into_localrw().await
    /// });
    /// ```
    pub fn into_localrw(self) -> IntoLocalRwDarcHandle<T> {
        // let wrapped_inner = WrappedInner {
        //     inner: NonNull::new(unsafe{self.inner.addr.as_mut_ptr()}).expect("invalid darc pointer"),
        // };
        let inner = self.inner.clone();
        let team = self.inner().team().clone();
        IntoLocalRwDarcHandle {
            darc: self.into(),
            team,
            launched: false,
            outstanding_future: Box::pin(async move {
                DarcInner::block_on_outstanding(inner, DarcMode::LocalRw, 0).await;
            }),
        }
    }

    #[doc(alias = "Collective")]
    /// Converts this Darc into a [GlobalRwDarc]
    ///
    /// This returns a handle (which is Future) thats needs to be `awaited` or `blocked` on to perform the operation.
    /// Awaiting/blocking on the handle is a blocking collective call amongst all PEs in the Darc's team, only returning once every PE in the team has completed the call.
    ///
    /// Furthermore, the handle will not return while any additional references outside of the one making this call exist on each PE. It is not possible for the
    /// pointed to object to wrapped by both a Darc and a LocalRwDarc simultaneously (on any PE).
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the `darc` to await/block the handle otherwise deadlock will occur (i.e. team barriers are being called internally)
    ///
    /// # Examples
    /// ```
    /// use lamellar::darc::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    ///
    /// let five = Darc::new(&world,5).block().expect("PE in world team");
    /// let five_as_globaldarc = five.into_globalrw().block();
    /// ```
    pub fn into_globalrw(self) -> IntoGlobalRwDarcHandle<T> {
        // let wrapped_inner = WrappedInner {
        //     inner: NonNull::new(unsafe{self.inner.addr.as_mut_ptr()}).expect("invalid darc pointer"),
        // };
        let inner = self.inner.clone();
        let team = self.inner().team().clone();
        IntoGlobalRwDarcHandle {
            darc: self.into(),
            team,
            launched: false,
            outstanding_future: Box::pin(async move {
                DarcInner::block_on_outstanding(inner, DarcMode::GlobalRw, 0).await;
            }),
        }
    }
}

impl<T> Clone for Darc<T> {
    fn clone(&self) -> Self {
        self.inner().local_cnt.fetch_add(1, Ordering::SeqCst);
        let id = self.inner().total_local_cnt.fetch_add(1, Ordering::SeqCst);
        trace! {"[{:?}] darc[{:?}][{id}] cloned from [{:?}] {:?} {:?} {:?}", std::thread::current().id(),self.inner().id,self.id,self.inner,self.inner().local_cnt.load(Ordering::SeqCst),self.inner().total_local_cnt.load(Ordering::SeqCst)};
        // self.print();
        Darc {
            inner: self.inner.clone(),
            src_pe: self.src_pe,
            id,
        }
    }
}

impl<T> Deref for Darc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        // println!(
        //     "[{:?}] deref called my address {:?}",
        //     std::thread::current().id(),
        //     self as *const _
        // );
        // println!(
        //     "[{:?}] deref called inner address: {:?}",
        //     std::thread::current().id(),
        //     self.inner
        // );
        // println!(
        //     "[{:?}] deref called item address: {:?}",
        //     std::thread::current().id(),
        //     self.inner().item
        // );
        // unsafe { &*self.inner().item }
        self.inner().item()
    }
}

impl<T: fmt::Display> fmt::Display for Darc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: fmt::Debug> fmt::Debug for Darc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

macro_rules! local_mode {
    ($mode:expr,$mode_refs:ident,$inner:ident) => {{
        let local_mode = unsafe {
            (*(((&mut $mode_refs[$inner.my_pe]) as *mut DarcMode) as *mut AtomicU64))
                .compare_exchange(
                    $mode as u64,
                    DarcMode::Dropping as u64,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
        };
        local_mode == Ok($mode as u64)
    }};
}

macro_rules! launch_drop {
    ($mode:ty, $inner:ident, $inner_ptr:expr) => {
        // debug!("launching drop task as {}", stringify!($mode));
        let team = $inner.team();
        // let mode_refs =
        //     unsafe { std::slice::from_raw_parts_mut($inner.mode_addr as *mut u8, $inner.num_pes) };
        let rdma = team.lamellae.comm();
        // let mut reqs = Vec::new();
        for pe in team.arch.team_iter() {
            // println!("darc block_on_outstanding put 3");
            // let _ = rdma.put(
            //     &team.scheduler,
            //     team.counters(),
            //     pe,
            //     $inner.mode_slice.sub_slice($inner.my_pe..=$inner.my_pe),
            //     // &mode_refs[$inner.my_pe..=$inner.my_pe],
            //     $inner.mode_slice.index_addr( $inner.my_pe),// * std::mem::size_of::<DarcMode>(),
            // ).spawn();
            let dropping = DarcMode::Dropping;
            // reqs.push(
                $inner.mode_slice.put_unmanaged(
                // &team.scheduler,
                // team.counters(),
                dropping,
                pe,
                $inner.my_pe,
            );
        // );
        }
        // for req in reqs.drain(..) {
        //     req.block();
        // }

        // team.print_cnt();
        team.team_counters.inc_outstanding(1);
        team.world_counters.inc_outstanding(1); //ensure we don't trigger any warnings in wait all
        let mut am = team.exec_am_local(DroppedWaitAM {
            inner: $inner_ptr,//CommAllocAddr::from($inner_addr as usize),
            // mode_slice: $inner.mode_slice.clone(),
            my_pe: $inner.my_pe,
            num_pes: $inner.num_pes,
            team: team.clone(),
            phantom: PhantomData::<T>,
        });
        am.launch();
        team.team_counters.dec_outstanding(1);
        team.world_counters.dec_outstanding(1);
    };
}

impl<T: 'static> Drop for Darc<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    fn drop(&mut self) {
        let inner = self.inner();
        let cnt = inner.local_cnt.fetch_sub(1, Ordering::SeqCst);
        debug! {"[{:?}] darc[{:?}][{:?}]  dropped {:?} {:?} {:?}",std::thread::current().id(),self.inner().id,self.id,self.inner,self.inner().local_cnt.load(Ordering::SeqCst),inner.total_local_cnt.load( Ordering::SeqCst)};

        if cnt == 0 {
            panic!("darc dropped too many times");
        }
        // self.print();
        if cnt == 1 {
            //we are currently the last local ref, if it increases again it must mean someone else has come in and we can probably let them worry about cleaning up...

            // println!(
            //     "[{:?}] Last local ref... for now! {:?}",
            //     std::thread::current().id(),
            //      self.inner().ref_cnt_slice
            // );
            // self.print();
            if self.inner().ref_cnt_slice.iter().any(|&x| x > 0) {
                //if we have received and accesses from remote pes, send we are finished
                inner.send_finished();
                // .into_iter().for_each(|x| {
                //     let _ = x.spawn();
                // });
            }
        }
        // debug!("in drop");
        // self.print();
        if inner.local_cnt.load(Ordering::SeqCst) == 0 {
            // we have no more current local references so lets try to launch our garbage collecting am

            // println!("[{:?}] launching drop task", std::thread::current().id());

            let mut mode_refs = self.inner().mode_slice.clone();
            if local_mode!(DarcMode::Darc, mode_refs, inner) {
                launch_drop!(DarcMode::Darc, inner, self.inner.clone());
            } else if local_mode!(DarcMode::LocalRw, mode_refs, inner) {
                launch_drop!(DarcMode::LocalRw, inner, self.inner.clone());
            } else if local_mode!(DarcMode::GlobalRw, mode_refs, inner) {
                launch_drop!(DarcMode::GlobalRw, inner, self.inner.clone());
            } else if local_mode!(DarcMode::UnsafeArray, mode_refs, inner) {
                launch_drop!(DarcMode::UnsafeArray, inner, self.inner.clone());
            } else if local_mode!(DarcMode::ReadOnlyArray, mode_refs, inner) {
                launch_drop!(DarcMode::ReadOnlyArray, inner, self.inner.clone());
            }
            // else if local_mode!(DarcMode::LocalOnlyArray, mode_refs, inner) {
            //     launch_drop!(DarcMode::LocalOnlyArray, inner, self.inner.clone());
            // }
            else if local_mode!(DarcMode::LocalLockArray, mode_refs, inner) {
                launch_drop!(DarcMode::LocalLockArray, inner, self.inner.clone());
            } else if local_mode!(DarcMode::GlobalLockArray, mode_refs, inner) {
                launch_drop!(DarcMode::GlobalLockArray, inner, self.inner.clone());
            } else if local_mode!(DarcMode::GenericAtomicArray, mode_refs, inner) {
                launch_drop!(DarcMode::GenericAtomicArray, inner, self.inner.clone());
            } else if local_mode!(DarcMode::NativeAtomicArray, mode_refs, inner) {
                launch_drop!(DarcMode::NativeAtomicArray, inner, self.inner.clone());
            } else if local_mode!(DarcMode::NetworkAtomicArray, mode_refs, inner) {
                launch_drop!(DarcMode::NetworkAtomicArray, inner, self.inner.clone());
            }
            // self.print();
        }
        // self.print();
    }
}

#[lamellar_impl::AmLocalDataRT]
struct DroppedWaitAM<T> {
    // inner_addr: CommAllocAddr,
    inner: DarcCommPtr<T>,
    // mode_slice: CommSlice<u64>,
    my_pe: usize,
    num_pes: usize,
    team: Pin<Arc<LamellarTeamRT>>, //we include this to insure the team isnt dropped until the darc has been fully dropped across the system.
    phantom: PhantomData<T>,
}

impl<T> std::fmt::Debug for DroppedWaitAM<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DroppedWaitAM {{ inner_addr: {:?}, mode_addr: {:?}, my_pe: {:?}, num_pes: {:?}, team: {:?} }}", self.inner.addr(), self.inner.mode_slice, self.my_pe, self.num_pes, self.team)
    }
}

unsafe impl<T> Send for DroppedWaitAM<T> {}
unsafe impl<T> Sync for DroppedWaitAM<T> {}

pub(crate) struct DarcCommPtr<T> {
    alloc: CommAlloc,
    _phantom: std::marker::PhantomData<DarcInner<T>>,
}
impl<T> std::fmt::Debug for DarcCommPtr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DarcCommPtr {{ alloc: {:?} }}", self.alloc)
    }
}

unsafe impl<T> Send for DarcCommPtr<T> {}
unsafe impl<T> Sync for DarcCommPtr<T> {}

impl<T> DarcCommPtr<T> {
    pub(crate) fn as_ptr(&self) -> *const DarcInner<T> {
        unsafe { self.alloc.as_ptr() }
    }
    pub(crate) fn as_mut_ptr(&self) -> *mut DarcInner<T> {
        unsafe { self.alloc.as_mut_ptr() }
    }
    pub(crate) fn as_ref(&self) -> Option<&DarcInner<T>> {
        unsafe { self.as_ptr().as_ref() }
    }
    pub(crate) fn as_mut(&self) -> Option<&mut DarcInner<T>> {
        unsafe { self.as_mut_ptr().as_mut() }
    }

    pub(crate) fn addr(&self) -> CommAllocAddr {
        self.alloc.comm_addr()
    }

    pub(crate) fn transmute<U>(&self) -> DarcCommPtr<U> {
        DarcCommPtr {
            alloc: CommAlloc {
                inner_alloc: self.alloc.inner_alloc.clone(),
                alloc_type: self.alloc.alloc_type,
            },
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Clone for DarcCommPtr<T> {
    fn clone(&self) -> Self {
        DarcCommPtr {
            alloc: CommAlloc {
                inner_alloc: self.alloc.inner_alloc.clone(),
                alloc_type: self.alloc.alloc_type,
            },
            _phantom: self._phantom,
        }
    }
}

impl<T> std::ops::Deref for DarcCommPtr<T> {
    type Target = DarcInner<T>;
    fn deref(&self) -> &Self::Target {
        unsafe { self.as_ptr().as_ref().unwrap() }
    }
}
impl<T> std::ops::DerefMut for DarcCommPtr<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.as_mut_ptr().as_mut().unwrap() }
    }
}

// pub(crate) struct WrappedInner<T> {
//     inner: NonNull<DarcInner<T>>,
// }
// unsafe impl<T: 'static> Send for WrappedInner<T> {}

// impl<T: 'static> Clone for WrappedInner<T> {
//     fn clone(&self) -> Self {
//         WrappedInner { inner: self.inner }
//     }
// }

// impl<T: 'static> std::fmt::Debug for WrappedInner<T> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "WrappedInner {{ inner: {:?} }}", unsafe {
//             self.inner.as_ref()
//         })
//     }
// }

// impl<T: 'static> std::ops::Deref for WrappedInner<T> {
//     type Target = DarcInner<T>;
//     fn deref(&self) -> &Self::Target {
//         unsafe { self.inner.as_ref() }
//     }
// }

// impl<T: 'static> std::ops::DerefMut for WrappedInner<T> {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         unsafe { self.inner.as_mut() }
//     }
// }

#[lamellar_impl::rt_am_local]
impl<T: 'static> LamellarAM for DroppedWaitAM<T> {
    #[tracing::instrument(skip_all, level = "debug")]
    async fn exec(self) {
        let mut timeout = std::time::Instant::now();

        trace!(
            "[{:?}] in DroppedWaitAM {:?} {:?} {:?}",
            std::thread::current().id(),
            self.inner,
            self.inner.id,
            self.inner.total_local_cnt.load(Ordering::SeqCst)
        );
        let block_on_fut =
            { DarcInner::block_on_outstanding(self.inner.clone(), DarcMode::Dropped, 0) };
        block_on_fut.await;

        trace!(
            "[{:?}] past block_on_outstanding {:?}",
            std::thread::current().id(),
            self.inner.as_ref()
        );
        for pe in self.inner.mode_slice.iter() {
            while *pe != DarcMode::Dropped {
                async_std::task::yield_now().await;

                if self.inner.local_cnt.load(Ordering::SeqCst) == 0 {
                    join_all(self.inner.send_finished()).await;
                }

                if timeout.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                    // let ref_cnts_slice = unsafe {
                    //     std::slice::from_raw_parts_mut(
                    //         wrapped.ref_cnt_addr as *mut usize,
                    //         wrapped.num_pes,
                    //     )
                    // };

                    println!("[{:?}][WARNING] -- Potential deadlock detected when trying to free distributed object.\n\
                        The runtime is currently waiting for all remaining references to this distributed object to be dropped.\n\
                        The current status of the object on each pe is {:?} with {:?} remaining local references and {:?} remaining remote references, ref cnts by pe {:?}\n\
                        the deadlock timeout can be set via the LAMELLAR_DEADLOCK_WARNING_TIMEOUT environment variable, the current timeout is {} seconds\n\
                        To view backtrace set RUST_LIB_BACKTRACE=1\n\
                        {}",
                        std::thread::current().id(),
                        self.inner.mode_slice.as_slice(),
                        self.inner.local_cnt.load(Ordering::SeqCst),
                        self.inner.ref_cnt_slice.as_slice(),
                        self.inner.dist_cnt.load(Ordering::SeqCst),
                        config().deadlock_warning_timeout,
                        std::backtrace::Backtrace::capture()
                    );
                    timeout = std::time::Instant::now();
                }
            }
        }
        trace!("after DarcMode::Dropped");
        // let inner =self.inner_addr as *mut DarcInner<T>;
        // let wrapped = unsafe{WrappedInner {
        //     inner: NonNull::new(self.inner_addr.as_mut_ptr())
        //         .expect("invalid darc pointer"),
        // }};

        // let inner = unsafe {&*wrapped.inner}; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc (but still allow async wait cause T is not send)
        unsafe {
            self.inner.valid.store(false, Ordering::SeqCst);
            while self.inner.dist_cnt.load(Ordering::SeqCst) != 0
                || self.inner.local_cnt.load(Ordering::SeqCst) != 0
            {
                if self.inner.local_cnt.load(Ordering::SeqCst) == 0 {
                    // wrapped.send_finished()
                    join_all(self.inner.send_finished()).await;
                }
                if timeout.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                    // let ref_cnts_slice = std::slice::from_raw_parts_mut(
                    //     wrapped.ref_cnt_addr as *mut usize,
                    //     wrapped.num_pes,
                    // );

                    println!("[{:?}][WARNING] --- Potential deadlock detected when trying to free distributed object.\n\
                        The runtime is currently waiting for all remaining references to this distributed object to be dropped.\n\
                        The current status of the object on each pe is {:?} with {:?} remaining local references and {:?} remaining remote references, ref cnts by pe {:?}\n\
                        the deadlock timeout can be set via the LAMELLAR_DEADLOCK_WARNING_TIMEOUT environment variable, the current timeout is {} seconds\n\
                        To view backtrace set RUST_LIB_BACKTRACE=1\n\
                        {}",
                        std::thread::current().id(),
                        self.inner.mode_slice.as_slice(),
                        self.inner.local_cnt.load(Ordering::SeqCst),
                        self.inner.ref_cnt_slice.as_slice(),
                        self.inner.dist_cnt.load(Ordering::SeqCst),
                        config().deadlock_warning_timeout,
                        std::backtrace::Backtrace::capture()
                    );
                    timeout = std::time::Instant::now();
                }
                async_std::task::yield_now().await;
            }

            trace!("going to drop object");

            if let Some(my_drop) = self.inner.drop {
                let mut dropped_done = false;
                while !dropped_done {
                    dropped_done = my_drop(&mut *(self.inner.item as *mut T));
                    async_std::task::yield_now().await;
                }
            }
            let _ = Box::from_raw(self.inner.item as *mut T);
            trace!("afterdrop object");

            while self.inner.weak_local_cnt.load(Ordering::SeqCst) != 0 {
                //we can't actually free the darc memory until all weak pointers are gone too
                async_std::task::yield_now().await;
            }
            let _team = Arc::from_raw(self.inner.team); //return to rust to drop appropriately
                                                        // println!("team cnt: {:?}", Arc::strong_count(&_team));
                                                        // println!("Darc freed! {:x} {:?}",self.inner_addr,mode_refs);
            let _am_counters = Arc::from_raw(self.inner.am_counters);
            let _barrier = Box::from_raw(self.inner.barrier);

            //need to make sure we free all the sub allocs so need to copy the inner data out first and then let it drop
            let mut darc_temp = std::mem::MaybeUninit::<DarcInner<T>>::uninit();

            std::ptr::copy_nonoverlapping(
                self.inner.alloc.as_ptr::<DarcInner<T>>(),
                darc_temp.as_mut_ptr(),
                1,
            );

            darc_temp.assume_init();
            self.team.lamellae.comm().wait();
            // now we can free the alloc
            self.team.lamellae.comm().free(self.inner.alloc.clone());
            debug!(
                "[{:?}]leaving DroppedWaitAM {:?}",
                std::thread::current().id(),
                self
            );
        }
    }
}

#[doc(hidden)]
#[derive(serde::Deserialize, serde::Serialize, Clone)]
pub struct __NetworkDarc {
    inner_addr: CommAllocAddr,
    backend: Backend,
    orig_world_pe: usize,
    orig_team_pe: usize,
}

impl std::fmt::Debug for __NetworkDarc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NetworkDarc {{ inner_addr: {:x}, backend: {:?}, orig_world_pe: {:?}, orig_team_pe: {:?} }}", self.inner_addr, self.backend, self.orig_world_pe, self.orig_team_pe)
    }
}

impl<T> From<Darc<T>> for __NetworkDarc {
    fn from(darc: Darc<T>) -> Self {
        trace!("net darc from darc id: {:?}", darc.id);
        trace!("darc  {:?}", darc.inner());
        let team = &darc.inner().team();
        let ndarc = __NetworkDarc {
            inner_addr: darc.inner.addr(),
            backend: team.lamellae.comm().backend(),
            orig_world_pe: team.world_pe,
            orig_team_pe: team.team_pe.expect("darcs only valid on team members"),
        };
        // darc.print();
        ndarc
    }
}

impl<T> From<&Darc<T>> for __NetworkDarc {
    fn from(darc: &Darc<T>) -> Self {
        trace!("net darc from &darc {:?}", darc.id);
        trace!("{:?}", std::backtrace::Backtrace::capture());
        trace!("darc  {:?}", darc.inner());
        let team = &darc.inner().team();
        let ndarc = __NetworkDarc {
            inner_addr: darc.inner.addr(),
            backend: team.lamellae.comm().backend(),
            orig_world_pe: team.world_pe,
            orig_team_pe: team.team_pe.expect("darcs only valid on team members"),
        };
        // darc.print();
        ndarc
    }
}

impl<T> From<__NetworkDarc> for Darc<T> {
    fn from(ndarc: __NetworkDarc) -> Self {
        if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend) {
            let local_addr = lamellae
                .comm()
                .local_addr(ndarc.orig_world_pe, ndarc.inner_addr.0);
            let alloc = lamellae
                .comm()
                .get_alloc(local_addr)
                .expect("alloc should be valid on remote PE");

            debug!("found alloc: {:?}", alloc);
            let inner = DarcCommPtr {
                alloc,
                _phantom: PhantomData,
            };
            debug!("inner: {:?}", inner.as_ref());
            inner.inc_pe_ref_count(ndarc.orig_team_pe, 1);
            inner.local_cnt.fetch_add(1, Ordering::SeqCst);
            let id = inner.total_local_cnt.fetch_add(1, Ordering::SeqCst);
            trace!("darc id: {} {:?}", id, inner.as_ref());
            let darc = Darc {
                inner,
                src_pe: ndarc.orig_team_pe,
                id,
            };
            darc
        } else {
            println!(
                "ndarc: 0x{:x} {:?} {:?} {:?} ",
                ndarc.inner_addr, ndarc.backend, ndarc.orig_world_pe, ndarc.orig_team_pe
            );
            panic!("unexepected lamellae backend {:?}", &ndarc.backend);
        }
    }
}
