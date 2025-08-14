use crate::{
    env_var::config,
    lamellae::{AllocationType, CommProgress, CommSlice, Lamellae},
    lamellar_arch::LamellarArchRT,
    lamellar_request::LamellarRequest,
    memregion::MemoryRegion,
    scheduler::Scheduler,
    warnings::RuntimeWarning,
};

use futures_util::Future;
use pin_project::{pin_project, pinned_drop};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll, Waker};
use std::time::Instant;
use tracing::trace;

pub(crate) struct Barrier {
    my_pe: usize, // global pe id
    num_pes: usize,
    n: usize, // dissemination factor
    num_rounds: usize,
    pub(crate) arch: Arc<LamellarArchRT>,
    pub(crate) scheduler: Arc<Scheduler>,
    lamellae: Arc<Lamellae>,
    barrier_cnt: AtomicUsize,
    cur_barrier_id: Arc<AtomicUsize>,
    barrier_buf: Arc<Vec<MemoryRegion<usize>>>,
    send_buf: Option<MemoryRegion<usize>>,
    panic: Arc<AtomicU8>,
}

impl Barrier {
    pub(crate) fn new(
        my_pe: usize,
        global_pes: usize,
        lamellae: Arc<Lamellae>,
        arch: Arc<LamellarArchRT>,
        scheduler: Arc<Scheduler>,
        panic: Arc<AtomicU8>,
    ) -> Barrier {
        let num_pes = arch.num_pes();
        // let mut n = std::env::var("LAMELLAR_BARRIER_DISSEMNATION_FACTOR")
        let mut n = config().barrier_dissemination_factor;
        let num_rounds = if n > 1 && num_pes > 2 {
            ((num_pes as f64).log2() / (n as f64).log2()).ceil() as usize
        } else {
            n = 1;
            (num_pes as f64).log2() as usize
        };
        let (buffs, send_buf) = if let Ok(_my_index) = arch.team_pe(my_pe) {
            if num_pes > 1 {
                let alloc = if global_pes == arch.num_pes() {
                    AllocationType::Global
                } else {
                    let mut pes = arch.team_iter().collect::<Vec<usize>>();
                    pes.sort();
                    AllocationType::Sub(pes)
                };
                trace!("creating barrier {:?}", alloc);
                let mut buffs = vec![];
                for r in 0..n {
                    trace!("[r: {r}] creating barrier buff {:?}, num_rounds: {num_rounds}", alloc);
                    buffs.push(MemoryRegion::new(
                        num_rounds,
                        &scheduler,
                        vec![],
                        &lamellae,
                        alloc.clone(),
                    ));
                }

                let send_buf = MemoryRegion::new(1, &scheduler, vec![], &lamellae, alloc);

                unsafe {
                    for buff in &buffs {
                        for elem in buff.as_mut_slice() {
                            *elem = 0;
                        }
                    }
                    for elem in send_buf.as_mut_slice() {
                        *elem = 0;
                    }
                }
                (buffs, Some(send_buf))
            } else {
                (vec![], None)
            }
        } else {
            (vec![], None)
        };

        let bar = Barrier {
            my_pe,
            num_pes,
            n,
            num_rounds,
            arch,
            scheduler,
            lamellae,
            barrier_cnt: AtomicUsize::new(1),
            cur_barrier_id: Arc::new(AtomicUsize::new(1)),
            barrier_buf: Arc::new(buffs),
            send_buf,
            panic,
        };
        // bar.print_bar();
        bar
    }

    fn print_bar(&self) {
        if let Some(send_buf) = &self.send_buf {
            let buffs = self
                .barrier_buf
                .iter()
                .map(|b| b.as_slice())
                .collect::<Vec<_>>();
            println!(
                " [LAMELLAR BARRIER][{:?}][{:?}][{:x}] {:?} {:?} {:?}",
                std::thread::current().id(),
                self.my_pe,
                self.barrier_buf[0].addr().unwrap(),
                buffs,
                send_buf.as_slice(),
                self.barrier_cnt.load(Ordering::SeqCst)
            );
        }
    }

    fn barrier_timeout(
        &self,
        s: &mut Instant,
        my_index: usize,
        round: usize,
        i: usize,
        team_recv_pe: isize,
        recv_pe: usize,
        send_buf_slice: &[usize],
    ) {
        RuntimeWarning::BarrierTimeout(s.elapsed().as_secs_f64()).print();

        if s.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
            println!(
                "[{:?}][{:?}, {:?}] round: {:?} i: {:?} teamsend_pe: {:?} team_recv_pe: {:?} recv_pe: {:?} id: {:?} buf {:?}",
                std::thread::current().id(),
                self.my_pe,
                my_index,
                round,
                i,
                (my_index + i * (self.n + 1).pow(round as u32))
                    % self.num_pes,
                team_recv_pe,
                recv_pe,
                send_buf_slice,
                    unsafe {
                        self.barrier_buf[i - 1]
                            .as_mut_slice()
                    }
            );
            self.print_bar();
            *s = Instant::now();
        }
    }

    fn barrier_internal<F: Fn()>(&self, wait_func: F) {
        // println!("in barrier");
        // self.print_bar();
        let mut s = Instant::now();
        if self.panic.load(Ordering::SeqCst) == 0 {
            if let Some(send_buf) = &self.send_buf {
                if let Ok(my_index) = self.arch.team_pe(self.my_pe) {
                    let send_buf_slice = unsafe {
                        // im the only thread (remote or local) that can write to this buff
                        send_buf.as_mut_slice()
                    };

                    let barrier_id = self.barrier_cnt.fetch_add(1, Ordering::SeqCst);
                    send_buf_slice[0] = barrier_id;
                    let barrier_slice = &[barrier_id];
                    trace!(
                        "[{:?}] barrier_id = {:?} buf_slice len{}",
                        std::thread::current().id(),
                        barrier_id,
                        send_buf_slice.len()
                    );
                    while barrier_id > self.cur_barrier_id.load(Ordering::SeqCst) {
                        wait_func();
                        if s.elapsed().as_secs_f64() > config().deadlock_warning_timeout {
                            break;
                        }
                    }

                    for round in 0..self.num_rounds {
                        for i in 1..=self.n {
                            let team_send_pe =
                                (my_index + i * (self.n + 1).pow(round as u32)) % self.num_pes;
                            if team_send_pe != my_index {
                                let send_pe = self.arch.single_iter(team_send_pe).next().unwrap();
                                // println!(
                                //     "[{:?}][ {:?} {:?}] round: {:?}  i: {:?} sending to [{:?} ({:?}) ] id: {:?} buf {:?}",
                                //     std::thread::current().id(),
                                //     self.my_pe,
                                //     my_index,
                                //     round,
                                //     i,
                                //     send_pe,
                                //     team_send_pe,
                                //     send_buf_slice,
                                //     unsafe {
                                //         self.barrier_buf[i - 1]
                                //             .as_mut_slice()
                                //             .expect("Data should exist on PE")
                                //     }
                                // );
                                // println!("barrier put_slice 1");
                                unsafe {
                                    let _ = self.barrier_buf[i - 1]
                                        .put_comm_slice(
                                            send_pe,
                                            round,
                                            CommSlice::from_slice(barrier_slice),
                                        )
                                        .spawn(); //no need to pass in counters as we wont leave until the barrier is complete anyway
                                                  //safe as we are the only ones writing to our index
                                }
                            }
                        }
                        for i in 1..=self.n {
                            let team_recv_pe = ((my_index as isize
                                - (i as isize * (self.n as isize + 1).pow(round as u32) as isize))
                                as isize)
                                .rem_euclid(self.num_pes as isize)
                                as isize;
                            let recv_pe =
                                self.arch.single_iter(team_recv_pe as usize).next().unwrap();
                            if team_recv_pe as usize != my_index {
                                trace!(
                                    "[{:?}][{:?} ] recv from [{:?} ({:?}) ] id: {:?} buf {:?}",
                                    std::thread::current().id(),
                                    self.my_pe,
                                    recv_pe,
                                    team_recv_pe,
                                    send_buf_slice,
                                    unsafe {
                                        self.barrier_buf[i - 1]
                                            .as_mut_slice()
                                    }
                                );
                                unsafe {
                                    //safe as  each pe is only capable of writing to its own index
                                    while self.barrier_buf[i - 1].as_mut_slice()[round] < barrier_id
                                    {
                                        self.barrier_timeout(
                                            &mut s,
                                            my_index,
                                            round,
                                            i,
                                            team_recv_pe,
                                            recv_pe,
                                            send_buf_slice,
                                        );
                                        self.lamellae.comm().flush();
                                        wait_func();
                                    }
                                }
                            }
                        }
                    }
                    self.cur_barrier_id.store(barrier_id + 1, Ordering::SeqCst);
                    // println!("leaving barrier {:?}", barrier_id);
                }
            }
        }

        // self.print_bar();
        // self.lamellae.comm().flush();
    }

    pub(crate) fn barrier(&self) {
        if std::thread::current().id() == *crate::MAIN_THREAD {
            self.barrier_internal(|| {
                // std::thread::yield_now();
                self.scheduler.exec_task();
            });
        } else {
            RuntimeWarning::BlockingCall("barrier", "async_barrier().await").print();
            self.tasking_barrier()
        }
    }

    // typically we want to block on the barrier and  spin on the barrier flags so that we can quick resume
    // but for the case of Darcs, or any case where the barrier is being called in a worker thread
    // we actually want to be able to process other tasks while the barrier is active
    pub(crate) fn tasking_barrier(&self) {
        // println!("calling tasking barrier");
        self.barrier_internal(|| {
            self.scheduler.exec_task();
        });
    }

    pub(crate) fn barrier_handle(&self) -> BarrierHandle {
        let mut handle = BarrierHandle {
            barrier_buf: self.barrier_buf.clone(),
            arch: self.arch.clone(),
            scheduler: self.scheduler.clone(),
            lamellae: self.lamellae.clone(),
            my_index: 0,
            num_pes: self.num_pes,
            barrier_id: 0,
            cur_barrier_id: self.cur_barrier_id.clone(),
            num_rounds: self.num_rounds,
            n: self.n,
            state: State::RoundInit(self.num_rounds),
            launched: false,
        };
        trace!("in barrier handle");
        // self.print_bar();
        if self.panic.load(Ordering::SeqCst) == 0 {
            if let Some(_) = &self.send_buf {
                if let Ok(my_index) = self.arch.team_pe(self.my_pe) {
                    let barrier_id = self.barrier_cnt.fetch_add(1, Ordering::SeqCst);
                    trace!("barrier id: {:?}", barrier_id);
                    handle.barrier_id = barrier_id;
                    handle.my_index = my_index;

                    if barrier_id > self.cur_barrier_id.load(Ordering::SeqCst) {
                        handle.state = State::Waiting;
                        return handle;
                    }
                    // else if barrier_id < self.cur_barrier_id.load(Ordering::SeqCst) {
                    //     println!("should this happen>?");
                    // }

                    handle.state = State::RoundInit(0);
                    let mut round = 0;
                    while round < self.num_rounds {
                        handle.do_send_round(round);
                        if let Some(recv_pe) = handle.do_recv_round(round, 1) {
                            handle.state = State::RoundInProgress(round, recv_pe);
                            return handle;
                        }
                        round += 1;
                    }
                    self.cur_barrier_id.store(barrier_id + 1, Ordering::SeqCst);
                    handle.state = State::RoundInit(self.num_rounds);
                }
            }
        }
        handle
    }

    pub(crate) async fn async_barrier(&self) {
        self.barrier_handle().await;
    }
}

// impl Drop for Barrier {
//     fn drop(&mut self) {
//         println!("dropping barrier");
//         println!("lamellae cnt: {:?}", Arc::strong_count(&self.lamellae));
//         println!("scheduler cnt: {:?}", Arc::strong_count(&self.scheduler));
//         println!("dropped barrier");
//     }
// }

#[pin_project(PinnedDrop)]
pub struct BarrierHandle {
    barrier_buf: Arc<Vec<MemoryRegion<usize>>>,
    arch: Arc<LamellarArchRT>,
    scheduler: Arc<Scheduler>,
    lamellae: Arc<Lamellae>,
    my_index: usize,
    num_pes: usize,
    pub(crate) barrier_id: usize,
    cur_barrier_id: Arc<AtomicUsize>,
    num_rounds: usize,
    n: usize,
    state: State,
    launched: bool,
}

#[pinned_drop]
impl PinnedDrop for BarrierHandle {
    fn drop(self: Pin<&mut Self>) {
        if !self.launched {
            RuntimeWarning::DroppedHandle("a BarrierHandle").print();
        }
    }
}

enum State {
    Waiting,
    RoundInit(usize),              //the round we are in
    RoundInProgress(usize, usize), //the round we are in, pe we are waiting to hear from
}

impl BarrierHandle {
    fn do_send_round(&self, round: usize) {
        // trace!("do send round {:?}", round);
        let barrier_slice = &[self.barrier_id];
        for i in 1..=self.n {
            let team_send_pe = (self.my_index + i * (self.n + 1).pow(round as u32)) % self.num_pes;
            if team_send_pe != self.my_index {
                let send_pe = self.arch.single_iter(team_send_pe).next().unwrap();
                unsafe {
                    let _ = self.barrier_buf[i - 1]
                        .put_comm_slice(send_pe, round, CommSlice::from_slice(barrier_slice))
                        .spawn();
                    //safe as we are the only ones writing to our index
                }
            }
        }
    }

    fn do_recv_round(&self, round: usize, recv_pe_index: usize) -> Option<usize> {
        // trace!("do recv round {:?}", round);
        for i in recv_pe_index..=self.n {
            let team_recv_pe = ((self.my_index as isize
                - (i as isize * (self.n as isize + 1).pow(round as u32) as isize))
                as isize)
                .rem_euclid(self.num_pes as isize) as isize;
            // let recv_pe = self.arch.single_iter(team_recv_pe as usize).next().unwrap();
            if team_recv_pe as usize != self.my_index {
                unsafe {
                    //safe as  each pe is only capable of writing to its own index
                    if self.barrier_buf[i - 1].as_mut_slice()[round] < self.barrier_id {
                        self.lamellae.comm().flush();
                        // trace!("waiting for recv pe: {:?} round: {:?} i: {:?}",  team_recv_pe, round, i);
                        return Some(i);
                    }
                }
            }
        }
        // trace!("dont need to wait for any recv pe");
        None
    }
}

impl Future for BarrierHandle {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // let mut this = self.project();
        self.launched = true;
        match self.state {
            State::Waiting => {
                if self.barrier_id > self.cur_barrier_id.load(Ordering::SeqCst) {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                // else if self.barrier_id < self.cur_barrier_id.load(Ordering::SeqCst) {
                //     println!("barrier id is less than cur barrier id");
                // }
                *self.project().state = State::RoundInit(0);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            State::RoundInit(round) => {
                let mut round = round;
                while round < self.num_rounds {
                    self.do_send_round(round);
                    if let Some(recv_pe) = self.do_recv_round(round, 1) {
                        // println!("waiting for pe {:?}", recv_pe);
                        *self.project().state = State::RoundInProgress(round, recv_pe);
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    round += 1;
                }
                self.cur_barrier_id
                    .store(self.barrier_id + 1, Ordering::SeqCst);
                *self.project().state = State::RoundInit(round);

                Poll::Ready(())
            }
            State::RoundInProgress(round, recv_pe) => {
                let mut round = round;
                if let Some(recv_pe) = self.do_recv_round(round, recv_pe) {
                    // println!("waiting for pe {:?}", recv_pe);
                    *self.project().state = State::RoundInProgress(round, recv_pe);
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                round += 1;
                while round < self.num_rounds {
                    self.do_send_round(round);
                    if let Some(recv_pe) = self.do_recv_round(round, 1) {
                        // println!("waiting for pe {:?}", recv_pe);
                        *self.project().state = State::RoundInProgress(round, recv_pe);
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    round += 1;
                }
                self.cur_barrier_id
                    .store(self.barrier_id + 1, Ordering::SeqCst);
                *self.project().state = State::RoundInit(round);

                Poll::Ready(())
            }
        }
    }
}

impl LamellarRequest for BarrierHandle {
    fn launch(&mut self) {
        self.launched = true;
    }
    fn blocking_wait(mut self) -> Self::Output {
        self.launched = true;
        match self.state {
            State::Waiting => {
                while self.barrier_id > self.cur_barrier_id.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }
                if self.barrier_id < self.cur_barrier_id.load(Ordering::SeqCst) {
                    println!("barrier id is less than cur barrier id");
                }
                let mut round = 0;
                while round < self.num_rounds {
                    self.do_send_round(round);
                    let mut recv_pe_index = 1;
                    while let Some(recv_pe) = self.do_recv_round(round, recv_pe_index) {
                        recv_pe_index = recv_pe;
                        std::thread::yield_now();
                    }
                    round += 1;
                }
                self.cur_barrier_id
                    .store(self.barrier_id + 1, Ordering::SeqCst);
            }
            State::RoundInit(round) => {
                let mut round = round;
                while round < self.num_rounds {
                    self.do_send_round(round);
                    let mut recv_pe_index = 1;
                    while let Some(recv_pe) = self.do_recv_round(round, recv_pe_index) {
                        recv_pe_index = recv_pe;
                        std::thread::yield_now();
                    }
                    round += 1;
                }
                self.cur_barrier_id
                    .store(self.barrier_id + 1, Ordering::SeqCst);
            }
            State::RoundInProgress(round, recv_pe) => {
                let mut round = round;
                let mut recv_pe_index = recv_pe;
                while let Some(_recv_pe) = self.do_recv_round(round, recv_pe_index) {
                    recv_pe_index = recv_pe;
                    std::thread::yield_now();
                }
                round += 1;
                while round < self.num_rounds {
                    recv_pe_index = 1;
                    while let Some(recv_pe) = self.do_recv_round(round, recv_pe_index) {
                        recv_pe_index = recv_pe;
                        std::thread::yield_now();
                    }
                    round += 1;
                }
                self.cur_barrier_id
                    .store(self.barrier_id + 1, Ordering::SeqCst);
            }
        }
    }

    fn ready_or_set_waker(&mut self, _waker: &Waker) -> bool {
        self.launched = true;
        match self.state {
            State::Waiting => false,
            State::RoundInit(round) => {
                if round < self.num_rounds {
                    false
                } else {
                    true
                }
            }
            State::RoundInProgress(round, _) => {
                if round < self.num_rounds {
                    false
                } else {
                    true
                }
            }
        }
    }

    fn val(&self) -> Self::Output {
        match self.state {
            State::Waiting => {
                while self.barrier_id > self.cur_barrier_id.load(Ordering::SeqCst) {
                    std::thread::yield_now();
                }
                // if self.barrier_id < self.cur_barrier_id.load(Ordering::SeqCst) {
                //     println!("barrier id is less than cur barrier id");
                // }
                let mut round = 0;
                while round < self.num_rounds {
                    self.do_send_round(round);
                    let mut recv_pe_index = 1;
                    while let Some(recv_pe) = self.do_recv_round(round, recv_pe_index) {
                        recv_pe_index = recv_pe;
                        std::thread::yield_now();
                    }
                    round += 1;
                }
                self.cur_barrier_id
                    .store(self.barrier_id + 1, Ordering::SeqCst);
            }
            State::RoundInit(round) => {
                let mut round = round;
                while round < self.num_rounds {
                    self.do_send_round(round);
                    let mut recv_pe_index = 1;
                    while let Some(recv_pe) = self.do_recv_round(round, recv_pe_index) {
                        recv_pe_index = recv_pe;
                        std::thread::yield_now();
                    }
                    round += 1;
                }
                self.cur_barrier_id
                    .store(self.barrier_id + 1, Ordering::SeqCst);
            }
            State::RoundInProgress(round, recv_pe) => {
                let mut round = round;
                let mut recv_pe_index = recv_pe;
                while let Some(recv_pe) = self.do_recv_round(round, recv_pe_index) {
                    recv_pe_index = recv_pe;
                    std::thread::yield_now();
                }
                round += 1;
                while round < self.num_rounds {
                    recv_pe_index = 1;
                    while let Some(recv_pe) = self.do_recv_round(round, recv_pe_index) {
                        recv_pe_index = recv_pe;
                        std::thread::yield_now();
                    }
                    round += 1;
                }
                self.cur_barrier_id
                    .store(self.barrier_id + 1, Ordering::SeqCst);
            }
        }
    }
}
