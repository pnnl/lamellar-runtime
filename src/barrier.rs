use crate::env_var::config;
use crate::lamellae::{AllocationType, Lamellae, LamellaeRDMA};
use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::MemoryRegion;
use crate::scheduler::Scheduler;

use futures_util::Future;
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;
use crate::lamellae::LamellaeComm;

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
                // println!("creating barrier {:?}", alloc);
                let mut buffs = vec![];
                for _ in 0..n {
                    buffs.push(MemoryRegion::new(
                        num_rounds,
                        lamellae.clone(),
                        alloc.clone(),
                    ));
                }

                let send_buf = MemoryRegion::new(1, lamellae.clone(), alloc);

                unsafe {
                    for buff in &buffs {
                        for elem in buff.as_mut_slice().expect("Data should exist on PE") {
                            *elem = 0;
                        }
                    }
                    for elem in send_buf.as_mut_slice().expect("Data should exist on PE") {
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
                .map(|b| b.as_slice().unwrap())
                .collect::<Vec<_>>();
            println!(
                " [LAMELLAR BARRIER][{:?}][{:?}][{:x}] {:?} {:?} {:?}",
                std::thread::current().id(),
                self.my_pe,
                self.barrier_buf[0].addr().unwrap(),
                buffs,
                send_buf.as_slice().expect("Data should exist on PE"),
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
        if s.elapsed().as_secs_f64() > config().deadlock_timeout {
            println!("[LAMELLAR WARNING][{:?}] Potential deadlock detected.\n\
            Barrier is a collective operation requiring all PEs associated with the distributed object to enter the barrier call.\n\
            Please refer to https://docs.rs/lamellar/latest/lamellar/index.html?search=barrier for more information\n\
            Note that barriers are often called internally for many collective operations, including constructing new LamellarTeams, LamellarArrays, and Darcs, as well as distributed iteration\n\
            You may be seeing this message if you have called barrier within an async context (meaning it was executed on a worker thread).\n\
            A full list of collective operations is found at https://docs.rs/lamellar/latest/lamellar/index.html?search=collective\n\
            The deadlock timeout can be set via the LAMELLAR_DEADLOCK_TIMEOUT environment variable, the current timeout is {} seconds\n\
            To view backtrace set RUST_LIB_BACKTRACE=1\n\
        {}",
        std::thread::current().id()
        ,config().deadlock_timeout,std::backtrace::Backtrace::capture());

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
                            .expect("Data should exist on PE")
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
                        send_buf.as_mut_slice().expect("Data should exist on PE")
                    };

                    let barrier_id = self.barrier_cnt.fetch_add(1, Ordering::SeqCst);
                    send_buf_slice[0] = barrier_id;
                    let barrier_slice = &[barrier_id];
                    // println!(
                    //     "[{:?}] barrier_id = {:?}",
                    //     std::thread::current().id(),
                    //     barrier_id
                    // );
                    while barrier_id > self.cur_barrier_id.load(Ordering::SeqCst) {
                        wait_func();
                        if s.elapsed().as_secs_f64() > config().deadlock_timeout {
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
                                    self.barrier_buf[i - 1].put_slice(
                                        send_pe,
                                        round,
                                        barrier_slice,
                                    ).block();
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
                                // println!(
                                //     "[{:?}][{:?} ] recv from [{:?} ({:?}) ] id: {:?} buf {:?}",
                                //     std::thread::current().id(),
                                //     self.my_pe,
                                //     recv_pe,
                                //     team_recv_pe,
                                //     send_buf_slice,
                                //     unsafe {
                                //         self.barrier_buf[i - 1]
                                //             .as_mut_slice()
                                //             .expect("Data should exist on PE")
                                //     }
                                // );
                                unsafe {
                                    //safe as  each pe is only capable of writing to its own index
                                    while self.barrier_buf[i - 1]
                                        .as_mut_slice()
                                        .expect("Data should exist on PE")[round]
                                        < barrier_id
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
                                        self.lamellae.flush();
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
        // self.lamellae.flush();
    }

    pub(crate) fn barrier(&self) {
        if std::thread::current().id() == *crate::MAIN_THREAD {
            self.barrier_internal(|| {
                // std::thread::yield_now();
                self.scheduler.exec_task();
            });
        } else {
            if let Some(val) = config().blocking_call_warning {
                // std::env::var("LAMELLAR_BLOCKING_CALL_WARNING") {
                // if val != "0" && val != "false" && val != "no" && val != "off" {
                if val {
                    println!("[LAMELLAR WARNING] You are calling barrier from within an async context, this is experimental and may result in deadlock! Using 'async_barrier().await;' is likely a better choice. Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning");
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling barrier from within an async context), this is experimental and may result in deadlock! Using 'async_barrier().await;' is likely a better choice. Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning");
            }
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
            lamellae: self.lamellae.clone(),
            my_index: 0,
            num_pes: self.num_pes,
            barrier_id: 0,
            cur_barrier_id: self.cur_barrier_id.clone(),
            num_rounds: self.num_rounds,
            n: self.n,
            state: State::RoundInit(self.num_rounds),
        };
        // println!("in barrier handle");
        // self.print_bar();
        if self.panic.load(Ordering::SeqCst) == 0 {
            if let Some(_) = &self.send_buf {
                if let Ok(my_index) = self.arch.team_pe(self.my_pe) {
                    let barrier_id = self.barrier_cnt.fetch_add(1, Ordering::SeqCst);
                    // println!("barrier id: {:?}", barrier_id);
                    handle.barrier_id = barrier_id;
                    handle.my_index = my_index;

                    // if barrier_id > self.cur_barrier_id.load(Ordering::SeqCst) {
                    handle.state = State::Waiting;
                    return handle;
                    // }
                    // else if barrier_id < self.cur_barrier_id.load(Ordering::SeqCst) {
                    //     println!("should this happen>?");
                    // }

                    // handle.state = State::RoundInit(0);
                    // let mut round = 0;
                    // while round < self.num_rounds {
                    //     handle.do_send_round_async(round);
                    //     if let Some(recv_pe) = handle.do_recv_round(round, 1) {
                    //         handle.state = State::RoundInProgress(round, recv_pe);
                    //         return handle;
                    //     }
                    //     round += 1;
                    // }
                    // self.cur_barrier_id.store(barrier_id + 1, Ordering::SeqCst);
                    // handle.state = State::RoundInit(self.num_rounds);
                }
            }
        }
        handle
    }

    pub(crate) async fn async_barrier(&self) {
        // println!("calling libfabasync barrier");
        // if let crate::lamellae::Lamellae::LibFabAsync(libfabasync) = self.lamellae.as_ref() {
        //     libfabasync.barrier().await;
        // }
        // else {
            self.barrier_handle().wait().await;
        // }
        // println!("Done calling libfabasync barrier");
    }

    //     pub(crate) async fn async_barrier(&self) {
    //         let mut s = Instant::now();
    //         if self.panic.load(Ordering::SeqCst) == 0 {
    //             if let Some(send_buf) = &self.send_buf {
    //                 if let Ok(my_index) = self.arch.team_pe(self.my_pe) {
    //                     let send_buf_slice = unsafe {
    //                         // im the only thread (remote or local) that can write to this buff
    //                         send_buf.as_mut_slice().expect("Data should exist on PE")
    //                     };

    //                     let barrier_id = self.barrier_cnt.fetch_add(1, Ordering::SeqCst);
    //                     send_buf_slice[0] = barrier_id;
    //                     let barrier_slice = &[barrier_id];

    //                     for round in 0..self.num_rounds {
    //                         for i in 1..=self.n {
    //                             let team_send_pe =
    //                                 (my_index + i * (self.n + 1).pow(round as u32)) % self.num_pes;
    //                             if team_send_pe != my_index {
    //                                 let send_pe = self.arch.single_iter(team_send_pe).next().unwrap();
    //                                 unsafe {
    //                                     self.barrier_buf[i - 1].put_slice(
    //                                         send_pe,
    //                                         round,
    //                                         barrier_slice,
    //                                     );
    //                                     //safe as we are the only ones writing to our index
    //                                 }
    //                             }
    //                         }
    //                         for i in 1..=self.n {
    //                             let team_recv_pe = ((my_index as isize
    //                                 - (i as isize * (self.n as isize + 1).pow(round as u32) as isize))
    //                                 as isize)
    //                                 .rem_euclid(self.num_pes as isize)
    //                                 as isize;
    //                             let recv_pe =
    //                                 self.arch.single_iter(team_recv_pe as usize).next().unwrap();
    //                             if team_recv_pe as usize != my_index {
    //                                 unsafe {
    //                                     //safe as  each pe is only capable of writing to its own index
    //                                     while self.barrier_buf[i - 1]
    //                                         .as_mut_slice()
    //                                         .expect("Data should exist on PE")[round]
    //                                         < barrier_id
    //                                     {
    //                                         self.barrier_timeout(
    //                                             &mut s,
    //                                             my_index,
    //                                             round,
    //                                             i,
    //                                             team_recv_pe,
    //                                             recv_pe,
    //                                             send_buf_slice,
    //                                         );
    //                                         self.lamellae.flush();
    //                                         async_std::task::yield_now().await;
    //                                     }
    //                                 }
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
}

// impl Drop for Barrier {
//     fn drop(&mut self) {
//         println!("dropping barrier");
//         println!("lamellae cnt: {:?}", Arc::strong_count(&self.lamellae));
//         println!("scheduler cnt: {:?}", Arc::strong_count(&self.scheduler));
//         println!("dropped barrier");
//     }
// }

#[pin_project]
pub struct BarrierHandle {
    barrier_buf: Arc<Vec<MemoryRegion<usize>>>,
    arch: Arc<LamellarArchRT>,
    lamellae: Arc<Lamellae>,
    my_index: usize,
    num_pes: usize,
    pub(crate) barrier_id: usize,
    cur_barrier_id: Arc<AtomicUsize>,
    num_rounds: usize,
    n: usize,
    state: State,
}

enum State {
    Waiting,
    RoundInit(usize),              //the round we are in
    RoundInProgress(usize, usize), //the round we are in, pe we are waiting to hear from
}

impl BarrierHandle {
    fn do_send_round(&self, round: usize) {
        // println!("do send round {:?}", round);
        let barrier_slice = &[self.barrier_id];
        for i in 1..=self.n {
            let team_send_pe = (self.my_index + i * (self.n + 1).pow(round as u32)) % self.num_pes;
            if team_send_pe != self.my_index {
                let send_pe = self.arch.single_iter(team_send_pe).next().unwrap();
                unsafe {
                    self.barrier_buf[i - 1].put_slice(send_pe, round, barrier_slice).block();
                    //safe as we are the only ones writing to our index
                }
            }
        }
    }

    async fn do_send_round_async(&self, round: usize) {
        // println!("do send round {:?}", round);
        let barrier_slice = &[self.barrier_id];
        for i in 1..=self.n {
            let team_send_pe = (self.my_index + i * (self.n + 1).pow(round as u32)) % self.num_pes;
            if team_send_pe != self.my_index {
                let send_pe = self.arch.single_iter(team_send_pe).next().unwrap();
                unsafe {
                    self.barrier_buf[i - 1].put_slice(send_pe, round, barrier_slice).await;
                    //safe as we are the only ones writing to our index
                }
            }
        }
    }

    fn do_recv_round(&self, round: usize, recv_pe_index: usize) -> Option<usize> {
        // println!("do recv round {:?}", round);
        for i in recv_pe_index..=self.n {
            let team_recv_pe = ((self.my_index as isize
                - (i as isize * (self.n as isize + 1).pow(round as u32) as isize))
                as isize)
                .rem_euclid(self.num_pes as isize) as isize;
            // let recv_pe = self.arch.single_iter(team_recv_pe as usize).next().unwrap();
            if team_recv_pe as usize != self.my_index {
                unsafe {
                    //safe as  each pe is only capable of writing to its own index
                    if self.barrier_buf[i - 1]
                        .as_mut_slice()
                        .expect("Data should exist on PE")[round]
                        < self.barrier_id
                    {
                        self.lamellae.flush();
                        return Some(i);
                    }
                }
            }
        }
        None
    }

    pub(crate) async fn wait(&mut self) {
        loop {
            match self.state {
                State::Waiting => {
                    while self.barrier_id > self.cur_barrier_id.load(Ordering::SeqCst) {
                        async_std::task::yield_now().await;
                    }
                    // else if self.barrier_id < self.cur_barrier_id.load(Ordering::SeqCst) {
                    //     println!("barrier id is less than cur barrier id");
                    // }
                    self.state = State::RoundInit(0);
                }
                State::RoundInit(round) => {
                    let mut round = round;
                    while round < self.num_rounds {
                        self.do_send_round_async(round).await;
                        loop{
                            if let Some(recv_pe) = self.do_recv_round(round, 1) {
                            // println!("waiting for pe {:?}", recv_pe);
                                self.state = State::RoundInProgress(round, recv_pe);
                                async_std::task::yield_now().await;
                            }
                            else {
                                break;
                            }
                        }
                        round += 1;
                    }
                    self.cur_barrier_id
                        .store(self.barrier_id + 1, Ordering::SeqCst);
                    self.state = State::RoundInit(round);
                    return;
                }
                State::RoundInProgress(round, recv_pe) => {
                    let mut round = round;
                    loop {
                        if let Some(recv_pe) = self.do_recv_round(round, recv_pe) {
                            // println!("waiting for pe {:?}", recv_pe);
                            self.state = State::RoundInProgress(round, recv_pe);
                            async_std::task::yield_now().await;
                        }
                        else {
                            break;
                        }
                    }
                    round += 1;
                    while round < self.num_rounds {
                        self.do_send_round_async(round).await;
                        loop {
                            if let Some(recv_pe) = self.do_recv_round(round, 1) {
                                // println!("waiting for pe {:?}", recv_pe);
                                self.state = State::RoundInProgress(round, recv_pe);
                                async_std::task::yield_now().await;
                            }
                            else {
                                break;
                            }
                        }
                        round += 1;
                    }
                    self.cur_barrier_id
                        .store(self.barrier_id + 1, Ordering::SeqCst);
                    self.state = State::RoundInit(round);
                    return;
                }
            }
        }
    }
}

impl Future for BarrierHandle {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // let mut this = self.project();
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
    fn blocking_wait(self) -> Self::Output {
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
