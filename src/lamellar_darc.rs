use core::marker::PhantomData;
use futures::Future;
use parking_lot::RwLock;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashSet;
use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::lamellar_world::LAMELLAES;
use crate::LamellarTeam;
// use crate::LamellarAM;
use crate::active_messaging::ActiveMessaging;
use crate::lamellae::{AllocationType, Backend, LamellaeComm, LamellaeRDMA};
use crate::IdError;

// lazy_static! {
//     pub(crate) static ref DARC_ADDRS: RwLock<HashSet<usize>> = RwLock::new(HashSet::new());
// }

#[lamellar_impl::AmDataRT(Debug)]
struct FinishedAm {
    cnt: usize,
    src_pe: usize,
    inner_addr: usize, //cant pass the darc itself cause we cant handle generics yet in lamellarAM...
}
#[lamellar_impl::rt_am]
impl LamellarAM for FinishedAm {
    fn exec() {
        // println!("in finished! {:?}",self);
        let inner = unsafe { &*(self.inner_addr as *mut DarcInner<()>) }; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc
        inner.dist_cnt.fetch_sub(self.cnt, Ordering::SeqCst);
    }
}

#[repr(C)]
pub struct DarcInner<T: ?Sized> {
    my_pe: usize,           // with respect to LamellarArch used to create this object
    num_pes: usize,         // with respect to LamellarArch used to create this object
    local_cnt: AtomicUsize, // cnt of times weve cloned for local access
    dist_cnt: AtomicUsize,  // cnt of times weve cloned (serialized) for distributed access
    ref_cnt_addr: usize,    // array of cnts for accesses from remote pes
    dropped_addr: usize,
    team: *const LamellarTeam,
    item: *const T,
}
unsafe impl<T: ?Sized + Sync + Send> Send for DarcInner<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for DarcInner<T> {}

pub struct Darc<T: 'static + ?Sized> {
    inner: *mut DarcInner<T>,
    src_pe: usize,
    // cur_pe: usize,
    phantom: PhantomData<DarcInner<T>>,
}

unsafe impl<T: ?Sized + Sync + Send> Send for Darc<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for Darc<T> {}

impl<T: ?Sized> DarcInner<T> {
    fn team(&self) -> Arc<LamellarTeam> {
        unsafe {
            // (*self.team).team.clone()
            Arc::increment_strong_count(self.team);
            Arc::from_raw(self.team)
        }
    }
    fn inc_pe_ref_count(&self, pe: usize, amt: usize) -> usize {
        //not sure yet what pe will be (world or team)
        if self.ref_cnt_addr + pe * std::mem::size_of::<AtomicUsize>() < 10 {
            println!("error!!!! addrress makes no sense: {:?} ", pe);
            println!("{:?}", self);
            panic!();
        }
        let team_pe = pe;
        let ref_cnt = unsafe {
            ((self.ref_cnt_addr + team_pe * std::mem::size_of::<AtomicUsize>()) as *mut AtomicUsize)
                .as_ref()
                .expect("invalid darc addr")
        };
        ref_cnt.fetch_add(amt, Ordering::SeqCst)
    }
    // fn dec_pe_ref_count(&self, pe: usize,amt: usize) -> usize {
    //     // println!("decrementing");
    //     let team_pe = pe;
    //     let  ref_cnt = unsafe{((self.ref_cnt_addr+team_pe*std::mem::size_of::<AtomicUsize>()) as *mut AtomicUsize).as_ref().unwrap()};
    //     ref_cnt.fetch_sub(amt,Ordering::SeqCst)
    // }

    fn update_item(&mut self, item: *const T) {
        self.item = item;
    }

    #[allow(dead_code)]
    fn item(&self) -> &T {
        unsafe { &(*self.item) }
    }

    fn send_finished(&self) -> Vec<Pin<Box<dyn Future<Output = Option<()>> + Send>>> {
        let ref_cnts = unsafe {
            std::slice::from_raw_parts_mut(self.ref_cnt_addr as *mut AtomicUsize, self.num_pes)
        };
        let team = self.team();
        let mut reqs = vec![];
        for pe in 0..ref_cnts.len() {
            let cnt = ref_cnts[pe].swap(0, Ordering::SeqCst);

            if cnt > 0 {
                let my_addr = &*self as *const DarcInner<T> as usize;
                let pe_addr = team.team.lamellae.remote_addr(
                    team.team.arch.world_pe(pe).expect("invalid team member"),
                    my_addr,
                );
                // println!("sending finished to {:?} {:?} team {:?} {:x}",pe,cnt,team.team.team_hash,my_addr);
                // println!("{:?}",self);
                reqs.push(
                    team.exec_am_pe(
                        pe,
                        FinishedAm {
                            cnt: cnt,
                            src_pe: pe,
                            inner_addr: pe_addr,
                        },
                    )
                    .into_future(),
                );
            }
        }
        reqs
    }
    unsafe fn any_ref_cnt(&self) -> bool {
        let ref_cnts =
            std::slice::from_raw_parts_mut(self.ref_cnt_addr as *mut usize, self.num_pes); //this is potentially a dirty read
        ref_cnts.iter().any(|x| *x > 0)
    }
    fn block_on_outstanding(&self, state: u8) {
        let mut timer = std::time::Instant::now();
        while self.dist_cnt.load(Ordering::SeqCst) > 0
            || self.local_cnt.load(Ordering::SeqCst) > 1
            || unsafe { self.any_ref_cnt() }
        {
            if self.local_cnt.load(Ordering::SeqCst) == 1 {
                self.send_finished();
            }
            if timer.elapsed().as_secs_f64() > 10.0 {
                println!("waiting for outstanding 1 {:?}", self);
                timer = std::time::Instant::now();
            }
            std::thread::yield_now();
        }
        let team = self.team();
        let dropped_refs =
            unsafe { std::slice::from_raw_parts_mut(self.dropped_addr as *mut u8, self.num_pes) };
        unsafe {
            (*(((&mut dropped_refs[self.my_pe]) as *mut u8) as *mut AtomicU8))
                .store(state, Ordering::SeqCst)
        };
        // (&dropped_refs[self.my_pe] = 2;
        let rdma = &team.team.lamellae;
        for pe in team.team.arch.team_iter() {
            rdma.put(
                pe,
                &dropped_refs[self.my_pe..=self.my_pe],
                self.dropped_addr + self.my_pe * std::mem::size_of::<u8>(),
            );
        }
        for pe in dropped_refs.iter() {
            while *pe != state {
                if self.local_cnt.load(Ordering::SeqCst) == 1 {
                    self.send_finished();
                }
                if timer.elapsed().as_secs_f64() > 10.0 {
                    println!("waiting for outstanding 2 {:?}", self);
                    timer = std::time::Instant::now();
                }
                std::thread::yield_now();
            }
        }
        while self.dist_cnt.load(Ordering::SeqCst) != 0
            || self.local_cnt.load(Ordering::SeqCst) > 1
            || unsafe { self.any_ref_cnt() }
        {
            if self.local_cnt.load(Ordering::SeqCst) == 1 {
                self.send_finished();
            }
            if timer.elapsed().as_secs_f64() > 10.0 {
                println!("waiting for outstanding 3 {:?}", self);
                timer = std::time::Instant::now();
            }
            std::thread::yield_now();
        }
        self.team().barrier();
    }
}

impl<T: ?Sized> fmt::Debug for DarcInner<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{:}/{:?}] lc: {:?} dc: {:?}\nref_cnt: {:?}\ndropped {:?}",
            self.my_pe,
            self.num_pes,
            self.local_cnt.load(Ordering::SeqCst),
            self.dist_cnt.load(Ordering::SeqCst),
            unsafe {
                &std::slice::from_raw_parts_mut(self.ref_cnt_addr as *mut usize, self.num_pes)
            },
            unsafe { &std::slice::from_raw_parts_mut(self.dropped_addr as *mut u8, self.num_pes) }
        )
    }
}

impl<T: ?Sized> Darc<T> {
    fn inner(&self) -> &DarcInner<T> {
        unsafe { self.inner.as_ref().expect("invalid darc inner ptr") }
    }
    fn inner_mut(&self) -> &mut DarcInner<T> {
        unsafe { self.inner.as_mut().expect("invalid darc inner ptr") }
    }
    fn ref_cnts_as_mut_slice(&self) -> &mut [usize] {
        let inner = self.inner();
        unsafe { std::slice::from_raw_parts_mut(inner.ref_cnt_addr as *mut usize, inner.num_pes) }
    }
    fn dropped_as_mut_slice(&self) -> &mut [u8] {
        let inner = self.inner();
        unsafe { std::slice::from_raw_parts_mut(inner.dropped_addr as *mut u8, inner.num_pes) }
    }

    pub fn serialize_update_cnts(&self, cnt: usize, cur_pe: usize) {
        // println!("serialize darc cnts");
        // if self.src_pe == cur_pe{
        self.inner()
            .dist_cnt
            .fetch_add(cnt, std::sync::atomic::Ordering::SeqCst);
        // self.print();
        // }
        // self.print();
        // println!("done serialize darc cnts");
    }

    pub fn deserialize_update_cnts(&self, cur_pe: usize) {
        // println!("deserialize darc? cnts");
        // if self.src_pe != cur_pe{
        self.inner().inc_pe_ref_count(self.src_pe, 1);
        // }
        self.inner().local_cnt.fetch_add(1, Ordering::SeqCst);
        // self.print();
        // println!("done deserialize darc cnts");
    }

    pub fn print(&self) {
        let rel_addr =
            unsafe { self.inner as usize - (*self.inner().team).team.lamellae.base_addr() };
        println!(
            "--------\norig: {:?} {:?} (0x{:x}) {:?}\n--------",
            self.src_pe,
            self.inner,
            rel_addr,
            self.inner()
        );
    }

    pub fn into_localrw(self) -> LocalRwDarc<T> {
        let inner = self.inner();
        let cur_pe = inner.team().team.world_pe;
        inner.block_on_outstanding(2);
        inner.local_cnt.fetch_add(1, Ordering::SeqCst);
        let item = unsafe { Box::from_raw(inner.item as *mut T) };
        let d = Darc {
            inner: self.inner as *mut DarcInner<RwLock<Box<T>>>,
            src_pe: self.src_pe,
            // cur_pe: cur_pe,
            phantom: PhantomData,
        };
        d.inner_mut()
            .update_item(Box::into_raw(Box::new(RwLock::new(item))));
        LocalRwDarc { darc: d }
    }
}

impl<T> Darc<T> {
    pub fn new(team: Arc<LamellarTeam>, item: T) -> Darc<T> {
        if let Ok(darc) = Darc::try_new(team, item, 0) {
            darc
        } else {
            panic!("Cannot create a team based Darc if not part of the team");
        }
    }

    pub fn try_new(team: Arc<LamellarTeam>, item: T, state: u8) -> Result<Darc<T>, IdError> {
        let team_rt = team.team.clone();
        let my_pe = team_rt.team_pe?;

        let alloc = if team_rt.num_pes == team_rt.num_world_pes {
            AllocationType::Global
        } else {
            AllocationType::Sub(team_rt.get_pes())
        };
        let size = std::mem::size_of::<DarcInner<T>>()
            + team_rt.num_pes * std::mem::size_of::<usize>()
            + team_rt.num_pes;
        let addr = team_rt.lamellae.alloc(size, alloc).expect("out of memory");
        let temp_team = team.clone();
        let darc_temp = DarcInner {
            my_pe: my_pe,
            num_pes: team_rt.num_pes,
            local_cnt: AtomicUsize::new(1),
            dist_cnt: AtomicUsize::new(0),
            ref_cnt_addr: addr + std::mem::size_of::<DarcInner<T>>(),
            dropped_addr: addr
                + std::mem::size_of::<DarcInner<T>>()
                + team_rt.num_pes * std::mem::size_of::<usize>(),
            team: Arc::into_raw(temp_team.clone()),
            item: Box::into_raw(Box::new(item)),
        };
        unsafe {
            std::ptr::copy_nonoverlapping(&darc_temp, addr as *mut DarcInner<T>, 1);
        }
        // DARC_ADDRS.write().insert(addr);

        let d = Darc {
            inner: addr as *mut DarcInner<T>,
            src_pe: my_pe,
            phantom: PhantomData,
        };
        // unsafe {
        for elem in d.dropped_as_mut_slice() {
            *elem = state;
        }
        // }
        d.print();
        team.barrier();
        Ok(d)
    }
}

impl<T: ?Sized> Clone for Darc<T> {
    fn clone(&self) -> Self {
        self.inner().local_cnt.fetch_add(1, Ordering::SeqCst);
        Darc {
            inner: self.inner,
            src_pe: self.src_pe,
            phantom: self.phantom,
        }
    }
}

impl<T: ?Sized> Deref for Darc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        unsafe { &*self.inner().item }
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for Darc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Darc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

// impl <'a, T> IntoIterator for &'a Darc<T>
// where
//     &'a T: IntoIterator{
//     type Item = <&'T as IntoIterator>::Item;
//     type IntoIter = <&'a T as IntoIterator>::IntoIter{
//         self.inner().item().into_iter()
//     }
// }

impl<T: 'static + ?Sized> Drop for Darc<T> {
    fn drop(&mut self) {
        let inner = self.inner();
        let cnt = inner.local_cnt.fetch_sub(1, Ordering::SeqCst);
        if cnt == 1 {
            //we are currently the last local ref, if it increases again it must mean someone else has come in and we can probably let them worry about cleaning up...

            let pe_ref_cnts = self.ref_cnts_as_mut_slice();
            // println!("Last local ref... for now! {:?}",pe_ref_cnts);
            // self.print();
            if pe_ref_cnts.iter().any(|&x| x > 0) {
                //if we have received and accesses from remote pes, send we are finished
                inner.send_finished();
            }
        }
        // println!("in drop");
        // self.print();
        if inner.local_cnt.load(Ordering::SeqCst) == 0 {
            // we have no more current local references so lets try to launch our garbage collecting am

            let dropped_refs = self.dropped_as_mut_slice();
            let local_dropped = unsafe {
                (*(((&mut dropped_refs[inner.my_pe]) as *mut u8) as *mut AtomicU8))
                    .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            };
            if local_dropped == Ok(0) {
                // println!("launching drop task");
                // self.print();
                inner.team().exec_am_local(DroppedWaitAM {
                    inner_addr: self.inner as *const u8 as usize,
                    dropped_addr: inner.dropped_addr,
                    my_pe: inner.my_pe,
                    num_pes: inner.num_pes,
                    phantom: PhantomData::<T>,
                });
            } else {
                let local_dropped = unsafe {
                    (*(((&mut dropped_refs[inner.my_pe]) as *mut u8) as *mut AtomicU8))
                        .compare_exchange(2, 1, Ordering::SeqCst, Ordering::SeqCst)
                };
                if local_dropped == Ok(2) {
                    // println!("launching drop task");
                    // self.print();
                    inner.team().exec_am_local(DroppedWaitAM {
                        inner_addr: self.inner as *const u8 as usize,
                        dropped_addr: inner.dropped_addr,
                        my_pe: inner.my_pe,
                        num_pes: inner.num_pes,
                        phantom: PhantomData::<T>,
                    });
                }
            }
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct DroppedWaitAM<T: ?Sized> {
    inner_addr: usize,
    dropped_addr: usize,
    my_pe: usize,
    num_pes: usize,
    phantom: PhantomData<T>,
}

unsafe impl<T: ?Sized> Send for DroppedWaitAM<T> {}
unsafe impl<T: ?Sized> Sync for DroppedWaitAM<T> {}

use std::ptr::NonNull;
struct Wrapper<T: ?Sized> {
    inner: NonNull<DarcInner<T>>,
}
unsafe impl<T: ?Sized> Send for Wrapper<T> {}

#[lamellar_impl::rt_am_local]
impl<T: 'static + ?Sized> LamellarAM for DroppedWaitAM<T> {
    fn exec(self) {
        // println!("in DroppedWaitAM");
        let dropped_refs =
            unsafe { std::slice::from_raw_parts_mut(self.dropped_addr as *mut u8, self.num_pes) };

        let mut timeout = std::time::Instant::now();
        let wrapped = Wrapper {
            inner: NonNull::new(self.inner_addr as *mut DarcInner<T>)
                .expect("invalid darc pointer"),
        };
        unsafe {
            // let inner = unsafe {&*wrapped.inner}; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc (but still allow async wait cause T is not send)
            while wrapped.inner.as_ref().dist_cnt.load(Ordering::SeqCst) != 0
                || wrapped.inner.as_ref().local_cnt.load(Ordering::SeqCst) != 0
            {
                if wrapped.inner.as_ref().local_cnt.load(Ordering::SeqCst) == 0 {
                    wrapped.inner.as_ref().send_finished();
                }
                if timeout.elapsed().as_secs_f64() > 5.0 {
                    println!(
                        "0. Darc trying to free! {:x} {:?} {:?} {:?}",
                        self.inner_addr,
                        dropped_refs,
                        wrapped.inner.as_ref().local_cnt.load(Ordering::SeqCst),
                        wrapped.inner.as_ref().dist_cnt.load(Ordering::SeqCst)
                    );
                    timeout = std::time::Instant::now();
                }
                async_std::task::yield_now().await;
            }
            let team = wrapped.inner.as_ref().team();
            let rdma = &team.team.lamellae;
            for pe in team.team.arch.team_iter() {
                // println!("putting {:?} to {:?} @ {:x}",&dropped_refs[self.my_pe..=self.my_pe],pe,self.dropped_addr + self.my_pe * std::mem::size_of::<u8>());
                rdma.put(
                    pe,
                    &dropped_refs[self.my_pe..=self.my_pe],
                    self.dropped_addr + self.my_pe * std::mem::size_of::<u8>(),
                );
            }
        }

        for pe in dropped_refs.iter() {
            while *pe != 1 {
                async_std::task::yield_now().await;
                unsafe {
                    if wrapped.inner.as_ref().local_cnt.load(Ordering::SeqCst) == 0 {
                        wrapped.inner.as_ref().send_finished();
                    }
                }
                if timeout.elapsed().as_secs_f64() > 5.0 {
                    println!(
                        "1. Darc trying to free! {:x} {:?}",
                        self.inner_addr, dropped_refs
                    );
                    timeout = std::time::Instant::now();
                }
            }
        }
        // let inner =self.inner_addr as *mut DarcInner<T>;
        let wrapped = Wrapper {
            inner: NonNull::new(self.inner_addr as *mut DarcInner<T>)
                .expect("invalid darc pointer"),
        };
        // let inner = unsafe {&*wrapped.inner}; //we dont actually care about the "type" we wrap here, we just need access to the meta data for the darc (but still allow async wait cause T is not send)
        unsafe {
            while wrapped.inner.as_ref().dist_cnt.load(Ordering::SeqCst) != 0
                || wrapped.inner.as_ref().local_cnt.load(Ordering::SeqCst) != 0
            {
                if wrapped.inner.as_ref().local_cnt.load(Ordering::SeqCst) == 0 {
                    wrapped.inner.as_ref().send_finished();
                }
                if timeout.elapsed().as_secs_f64() > 5.0 {
                    println!(
                        "2. Darc trying to free! {:x} {:?} {:?} {:?}",
                        self.inner_addr,
                        dropped_refs,
                        wrapped.inner.as_ref().local_cnt.load(Ordering::SeqCst),
                        wrapped.inner.as_ref().dist_cnt.load(Ordering::SeqCst)
                    );
                    timeout = std::time::Instant::now();
                }
                async_std::task::yield_now().await;
            }
            // let inner = unsafe {&*(self.inner_addr as *mut DarcInner<T>)}; //now we need to true type to deallocate appropriately
            let _item = Box::from_raw(wrapped.inner.as_ref().item as *mut T);
            let team = Arc::from_raw(wrapped.inner.as_ref().team); //return to rust to drop appropriately
                                                                   // println!("Darc freed! {:x} {:?}",self.inner_addr,dropped_refs);
            team.team.lamellae.free(self.inner_addr);
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct __NetworkDarc<T: ?Sized> {
    inner_addr: usize,
    backend: Backend,
    orig_world_pe: usize,
    orig_team_pe: usize,
    phantom: PhantomData<DarcInner<T>>,
}

pub fn darc_serialize<S, T>(darc: &Darc<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized,
{
    __NetworkDarc::from(darc).serialize(s)
}

pub fn darc_from_ndarc<'de, D, T>(deserializer: D) -> Result<Darc<T>, D::Error>
where
    D: Deserializer<'de>,
    T: ?Sized,
{
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    Ok(Darc::from(ndarc))
}

impl<T: ?Sized> From<&Darc<T>> for __NetworkDarc<T> {
    fn from(darc: &Darc<T>) -> Self {
        // println!("net darc from darc");
        let team = &darc.inner().team().team;
        let ndarc = __NetworkDarc {
            inner_addr: darc.inner as *const u8 as usize,
            backend: team.lamellae.backend(),
            orig_world_pe: team.world_pe,
            // orig_team_pe: darc.src_pe,
            orig_team_pe: team.team_pe.expect("darcs only valid on team members"),
            phantom: PhantomData,
        };
        // darc.print();
        ndarc
    }
}

impl<T: ?Sized> From<__NetworkDarc<T>> for Darc<T> {
    fn from(ndarc: __NetworkDarc<T>) -> Self {
        // println!("ndarc: 0x{:x} {:?} {:?} {:?} ",ndarc.inner_addr,ndarc.backend,ndarc.orig_world_pe,ndarc.orig_team_pe);
        if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend) {
            let darc = Darc {
                inner: lamellae.local_addr(ndarc.orig_world_pe, ndarc.inner_addr)
                    as *mut DarcInner<T>,
                src_pe: ndarc.orig_team_pe,
                phantom: PhantomData,
            };
            // if !DARC_ADDRS.read().contains(&ndarc.inner_addr)
            //     && !DARC_ADDRS.read().contains(&(darc.inner as usize))
            // {
            //     println!(
            //         "wtf! 0x{:x} -- 0x{:x} (0x{:x}) ",
            //         ndarc.inner_addr,
            //         darc.inner as usize,
            //         darc.inner as usize - lamellae.base_addr()
            //     );
            //     for addr in DARC_ADDRS.read().iter() {
            //         println!("0x{:x} (0x{:x}) ", addr, addr - lamellae.base_addr());
            //     }
            // }
            // darc.print();
            darc
        } else {
            panic!("unexepected lamellae backend {:?}", &ndarc.backend);
        }
    }
}

#[derive(Debug)]
pub struct LocalRwDarc<T: 'static + ?Sized> {
    darc: Darc<RwLock<Box<T>>>,
}

unsafe impl<T: ?Sized + Sync + Send> Send for LocalRwDarc<T> {}
unsafe impl<T: ?Sized + Sync + Send> Sync for LocalRwDarc<T> {}
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
impl<T: ?Sized> LocalRwDarc<T> {
    fn inner(&self) -> &DarcInner<RwLock<Box<T>>> {
        self.darc.inner()
    }

    pub fn serialize_update_cnts(&self, cnt: usize, cur_pe: usize) {
        // println!("serialize darc cnts");
        // if self.darc.src_pe == cur_pe{
        self.inner()
            .dist_cnt
            .fetch_add(cnt, std::sync::atomic::Ordering::SeqCst);
        // }
        // self.print();
        // println!("done serialize darc cnts");
    }

    pub fn deserialize_update_cnts(&self, cur_pe: usize) {
        // println!("deserialize darc? cnts");
        // if self.darc.src_pe != cur_pe{
        self.inner().inc_pe_ref_count(self.darc.src_pe, 1); // we need to increment by 2 cause bincode calls the serialize function twice when serializing...
                                                            // }
        self.inner().local_cnt.fetch_add(1, Ordering::SeqCst);
        // self.print();
        // println!("done deserialize darc cnts");
    }

    pub fn print(&self) {
        let rel_addr =
            unsafe { self.darc.inner as usize - (*self.inner().team).team.lamellae.base_addr() };
        println!(
            "--------\norig: {:?} {:?} (0x{:x}) {:?}\n--------",
            self.darc.src_pe,
            self.darc.inner,
            rel_addr,
            self.inner()
        );
    }

    pub fn read(&self) -> RwLockReadGuard<Box<T>> {
        self.darc.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<Box<T>> {
        self.darc.write()
    }

    pub fn into_darc(self) -> Darc<T> {
        let inner = self.inner();
        // println!("into_darc");
        // self.print();
        inner.block_on_outstanding(0);
        // println!("after block on outstanding");
        inner.local_cnt.fetch_add(1, Ordering::SeqCst);
        let item = unsafe { Box::from_raw(inner.item as *mut RwLock<Box<T>>).into_inner() };
        let d = Darc {
            inner: self.darc.inner as *mut DarcInner<T>,
            src_pe: self.darc.src_pe,
            phantom: PhantomData,
        };
        d.inner_mut().update_item(Box::into_raw(item));
        d
    }
}

impl<T> LocalRwDarc<T> {
    pub fn new(team: Arc<LamellarTeam>, item: T) -> LocalRwDarc<T> {
        if let Ok(darc) = Darc::try_new(team.clone(), RwLock::new(Box::new(item)), 2) {
            LocalRwDarc { darc: darc }
        } else {
            panic!("Cannot create a team based Darc if not part of the team");
        }
    }

    pub fn try_new(team: Arc<LamellarTeam>, item: T) -> Result<LocalRwDarc<T>, IdError> {
        Ok(LocalRwDarc {
            darc: Darc::try_new(team, RwLock::new(Box::new(item)), 2)?,
        })
    }
}

impl<T: ?Sized> Clone for LocalRwDarc<T> {
    fn clone(&self) -> Self {
        // self.inner().local_cnt.fetch_add(1,Ordering::SeqCst);
        LocalRwDarc {
            darc: self.darc.clone(),
        }
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for LocalRwDarc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self.read(), f)
    }
}

pub fn localrw_serialize<S, T>(localrw: &LocalRwDarc<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: ?Sized,
{
    __NetworkDarc::<T>::from(&localrw.darc).serialize(s)
}

pub fn localrw_from_ndarc<'de, D, T>(deserializer: D) -> Result<LocalRwDarc<T>, D::Error>
where
    D: Deserializer<'de>,
    T: ?Sized,
{
    let ndarc: __NetworkDarc<T> = Deserialize::deserialize(deserializer)?;
    let rwdarc = LocalRwDarc {
        darc: Darc::from(ndarc),
    };
    // println!("lrwdarc from net darc");
    // rwdarc.print();
    Ok(rwdarc)
}

impl<T: ?Sized> From<&Darc<RwLock<Box<T>>>> for __NetworkDarc<T> {
    fn from(darc: &Darc<RwLock<Box<T>>>) -> Self {
        // println!("rwdarc to net darc");
        // darc.print();
        let team = &darc.inner().team().team;
        let ndarc = __NetworkDarc {
            inner_addr: darc.inner as *const u8 as usize,
            backend: team.lamellae.backend(),
            orig_world_pe: team.world_pe,
            orig_team_pe: team.team_pe.expect("darcs only valid on team members"),
            phantom: PhantomData,
        };
        ndarc
    }
}

impl<T: ?Sized> From<__NetworkDarc<T>> for Darc<RwLock<Box<T>>> {
    fn from(ndarc: __NetworkDarc<T>) -> Self {
        // println!("rwdarc from net darc");

        if let Some(lamellae) = LAMELLAES.read().get(&ndarc.backend) {
            let darc = Darc {
                inner: lamellae.local_addr(ndarc.orig_world_pe, ndarc.inner_addr)
                    as *mut DarcInner<RwLock<Box<T>>>,
                src_pe: ndarc.orig_team_pe,
                phantom: PhantomData,
            };
            // if !DARC_ADDRS.read().contains(&ndarc.inner_addr)
            //     && !DARC_ADDRS.read().contains(&(darc.inner as usize))
            // {
            //     println!(
            //         "wtf! 0x{:x} -- 0x{:x} (0x{:x}) ",
            //         ndarc.inner_addr,
            //         darc.inner as usize,
            //         darc.inner as usize - lamellae.base_addr()
            //     );
            //     for addr in DARC_ADDRS.read().iter() {
            //         println!("0x{:x} (0x{:x}) ", addr, addr - lamellae.base_addr());
            //     }
            // }
            // panic!("just for debugging");
            // darc.print();
            darc
        } else {
            panic!("unexepected lamellae backend {:?}", &ndarc.backend);
        }
    }
}
