use crate::lamellae::{AllocationType, Lamellae};
use crate::lamellar_arch::LamellarArchRT;
// use crate::lamellar_memregion::{SharedMemoryRegion,RegisteredMemoryRegion};
use crate::memregion::MemoryRegion; //, RTMemoryRegionRDMA, RegisteredMemoryRegion};
use crate::scheduler::{Scheduler, SchedulerQueue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub(crate) struct Barrier {
    my_pe: usize, // global pe id
    pub(crate) arch: Arc<LamellarArchRT>,
    pub(crate) scheduler: Arc<Scheduler>,
    barrier_cnt: AtomicUsize,
    barrier_buf: Option<SubBufs>,
}

struct SubBufs {
    barrier1: MemoryRegion<usize>,
    barrier2: MemoryRegion<usize>,
    barrier3: MemoryRegion<usize>,
}

impl Barrier {
    pub(crate) fn new(
        my_pe: usize,
        global_pes: usize,
        lamellae: Arc<Lamellae>,
        arch: Arc<LamellarArchRT>,
        scheduler: Arc<Scheduler>,
    ) -> Barrier {
        let bufs = if let Ok(_my_index) = arch.team_pe(my_pe) {
            let num_pes = arch.num_pes;
            if num_pes > 1 {
                let alloc = if global_pes == arch.num_pes {
                    AllocationType::Global
                } else {
                    let mut pes = arch.team_iter().collect::<Vec<usize>>();
                    pes.sort();
                    AllocationType::Sub(pes)
                };
                // println!("creating barrier {:?}",alloc);
                let barrier1 = MemoryRegion::new(num_pes, lamellae.clone(), alloc.clone());
                let barrier2 = MemoryRegion::new(num_pes, lamellae.clone(), alloc.clone());
                let barrier3 = MemoryRegion::new(3, lamellae.clone(), alloc);
                unsafe {
                    for elem in barrier1.as_mut_slice().unwrap() {
                        *elem = 0;
                    }
                    for elem in barrier2.as_mut_slice().unwrap() {
                        *elem = 0;
                    }
                    for elem in barrier3.as_mut_slice().unwrap() {
                        *elem = 0;
                    }
                }
                Some(SubBufs {
                    barrier1: barrier1,
                    barrier2: barrier2,
                    barrier3: barrier3,
                })
            } else {
                None
            }
        } else {
            None
        };
        let bar = Barrier {
            my_pe: my_pe,
            arch: arch,
            scheduler: scheduler,
            barrier_cnt: AtomicUsize::new(0),
            barrier_buf: bufs,
        };
        // bar.print_bar();
        bar
    }

    fn print_bar(&self) {
        if let Some(bufs) = &self.barrier_buf {
            println!(
                "[{:?}] [LAMELLAR BARRIER] {:?} {:?} {:?}",
                self.my_pe,
                bufs.barrier1.as_slice(),
                bufs.barrier2.as_slice(),
                bufs.barrier3.as_slice()
            );
        }
    }

    fn check_barrier_vals(&self, barrier_id: usize, barrier_buf: &MemoryRegion<usize>) {
        let mut s = Instant::now();
        for pe in barrier_buf.as_slice().unwrap() {
            while *pe != barrier_id {
                // std::thread::yield_now();
                self.scheduler.exec_task();
                if s.elapsed().as_secs_f64() > *crate::DEADLOCK_TIMEOUT {
                    println!("[WARNING] Potential deadlock detected.\n\
                    Barrier is a collective operation requiring all PEs associated with the distributed object to enter the barrier call.\n\
                    Please refer to https://docs.rs/lamellar/latest/lamellar/index.html?search=barrier for more information\n\
                    Note that barriers are often called internally for many collective operations, including constructing new LamellarTeams, LamellarArrays, and Darcs, as well as distributed iteration\n\
                    A full list of collective operations is found at https://docs.rs/lamellar/latest/lamellar/index.html?search=collective\n\
                    The deadlock timeout can be set via the LAMELLAR_DEADLOCK_TIMEOUT environment variable, the current timeout is {} seconds\n\
                    To view backtrace set RUST_LIB_BACKTRACE=1\n\
                    {}",*crate::DEADLOCK_TIMEOUT,std::backtrace::Backtrace::capture());
                    self.print_bar();
                    s = Instant::now();
                }
            }
        }
    }

    fn put_barrier_val(
        &self,
        my_index: usize,
        barrier_id: &[usize],
        barrier_buf: &MemoryRegion<usize>,
    ) {
        for world_pe in self.arch.team_iter() {
            unsafe {
                // println!("world_pe {:?} my_index {:?}",world_pe,my_index);
                barrier_buf.put_slice(world_pe, my_index, barrier_id);
            }
        }
    }
    pub(crate) fn barrier(&self) {
        if let Some(bufs) = &self.barrier_buf {
            if let Ok(my_index) = self.arch.team_pe(self.my_pe) {
                let mut barrier_id = self.barrier_cnt.fetch_add(1, Ordering::SeqCst);
                self.check_barrier_vals(barrier_id, &bufs.barrier2);
                barrier_id += 1;
                let barrier3_slice = unsafe { bufs.barrier3.as_mut_slice().unwrap() };
                barrier3_slice[0] = barrier_id;
                let barrier_slice = &[barrier_id];
                self.put_barrier_val(my_index, barrier_slice, &bufs.barrier1);
                self.check_barrier_vals(barrier_id, &bufs.barrier1);
                barrier3_slice[1] = barrier_id;
                let barrier_slice = &barrier3_slice[1..2];
                self.put_barrier_val(my_index, barrier_slice, &bufs.barrier2);
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
