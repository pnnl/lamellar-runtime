use crate::env_var::config;
use crate::lamellae::{AllocationType, Lamellae, LamellaeRDMA};
use crate::lamellar_arch::LamellarArchRT;
use crate::memregion::MemoryRegion;
use crate::scheduler::Scheduler;

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub(crate) struct Barrier {
    my_pe: usize, // global pe id
    num_pes: usize,
    n: usize, // dissemination factor
    num_rounds: usize,
    pub(crate) arch: Arc<LamellarArchRT>,
    pub(crate) scheduler: Arc<Scheduler>,
    lamellae: Arc<Lamellae>,
    barrier_cnt: AtomicUsize,
    barrier_buf: Vec<MemoryRegion<usize>>,
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
            barrier_buf: buffs,
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
                                    );
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
                }
            }
        }
        // println!("leaving barrier");
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
            if let Some(val) = config().barrier_warning {
                // std::env::var("LAMELLAR_BARRIER_WARNING") {
                // if val != "0" && val != "false" && val != "no" && val != "off" {
                if val {
                    println!("[LAMELLAR WARNING] You are calling barrier from within an async context, this is experimental and may result in deadlock! Using 'async_barrier().await;' is likely a better choice. Set LAMELLAR_BARRIER_WARNING=0 to disable this warning");
                }
            } else {
                println!("[LAMELLAR WARNING] You are calling barrier from within an async context), this is experimental and may result in deadlock! Using 'async_barrier().await;' is likely a better choice. Set LAMELLAR_BARRIER_WARNING=0 to disable this warning");
            }
            self.tasking_barrier()
        }
    }

    // typically we want to block on the barrier and  spin on the barrier flags so that we can quick resume
    // but for the case of Darcs, or any case where the barrier is being called in a worker thread
    // we actually want to be able to process other tasks while the barrier is active
    pub(crate) fn tasking_barrier(&self) {
        self.barrier_internal(|| {
            self.scheduler.exec_task();
        });
    }

    pub(crate) async fn async_barrier(&self) {
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
                                // println!("barrier put_slice 2");
                                unsafe {
                                    self.barrier_buf[i - 1].put_slice(
                                        send_pe,
                                        round,
                                        barrier_slice,
                                    );
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
                                        async_std::task::yield_now().await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// impl Drop for Barrier {
//     fn drop(&mut self) {
//         //println!("dropping barrier");
//         // println!("arch: {:?}",Arc::strong_count(&self.arch));
//         //println!("dropped barrier");
//     }
// }
