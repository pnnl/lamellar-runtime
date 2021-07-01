use crate::lamellae::{AllocationType,Lamellae};
use crate::lamellar_arch::LamellarArchRT;
use crate::lamellar_memregion::{LamellarMemoryRegion,RegisteredMemoryRegion};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;


pub(crate) struct Barrier{
    my_pe: usize, // global pe id
    pub(crate) arch: Arc<LamellarArchRT>,
    barrier_cnt: AtomicUsize,
    barrier_buf: Option<SubBufs>,
    
}

struct SubBufs{
    barrier1: LamellarMemoryRegion<usize>,
    barrier2: LamellarMemoryRegion<usize>,
    barrier3: LamellarMemoryRegion<usize>,
    _orig_buf: LamellarMemoryRegion<usize>, //needs to be last so we can drop appropriately...
}

impl Barrier{

    pub(crate) fn new(my_pe: usize, global_pes: usize, lamellae: Arc<Lamellae>, arch: Arc<LamellarArchRT>) -> Barrier{
        let bufs = if let Ok(_my_index) = arch.team_pe(my_pe) {
            let num_pes = arch.num_pes;
            if num_pes > 1 {
                let size = num_pes * 2 + 3;
                let alloc = if global_pes==arch.num_pes {
                    AllocationType::Global
                }
                else{ 
                    AllocationType::Sub(arch.team_iter().collect::<Vec<usize>>())
                };
                let data_buf = LamellarMemoryRegion::new(size,lamellae.clone(),alloc);
                unsafe { 
                    for elem in data_buf.as_mut_slice().unwrap() {
                        *elem = 0;
                    }
                }
                Some(SubBufs{
                    barrier1: data_buf.sub_region(0..num_pes),
                    barrier2: data_buf.sub_region(num_pes..num_pes*2),
                    barrier3: data_buf.sub_region(num_pes*2..),
                    _orig_buf: data_buf.clone(),
                })
            }
            else{
                None
            }
        }
        else{
            None
        };
        let bar = Barrier{
            my_pe: my_pe,
            arch: arch,
            barrier_cnt: AtomicUsize::new(0),
            barrier_buf: bufs,
        };
        bar.print_bar();
        bar
    }

    fn print_bar(&self){
        if let Some(bufs) = &self.barrier_buf {
            println!("[{:?}] [LAMELLAR BARRIER] {:?} {:?} {:?}",
                    self.my_pe, bufs.barrier1.as_slice(),
                    bufs.barrier2.as_slice(),bufs.barrier3.as_slice());
        } 
    }

    fn check_barrier_vals(&self, barrier_id: usize, barrier_buf: &LamellarMemoryRegion<usize>) {
        let mut s = Instant::now();
        for pe in barrier_buf.as_slice().unwrap() {
            while *pe != barrier_id {
                std::thread::yield_now();
                if s.elapsed().as_secs_f64() > 10.0 {
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
        barrier_buf: &LamellarMemoryRegion<usize>,
    ) {
        for world_pe in self.arch.team_iter() {
            unsafe {
                // println!("putting {:?} into {:?} on {:?}",barrier_id, my_index, world_pe);
                barrier_buf.put_slice(world_pe, my_index, barrier_id);
            }
        }
    }
    pub(crate) fn barrier(&self) {
        // #[cfg(test)]
        // if self.lamellae.backend() == crate::lamellae::Backend::Local {
        //     return;
        // }
        // println!("[{:?}] in barrier ",self.barrier_cnt.load(Ordering::SeqCst));
        // self.print_bar();
        if let Some(bufs) = &self.barrier_buf{
            if let Ok(my_index) = self.arch.team_pe(self.my_pe) {  
                // self.bar_print("bar_init".to_string());
                let mut barrier_id = self.barrier_cnt.fetch_add(1, Ordering::SeqCst);
                // println!("[{:?}] checking barrier entry ({:?}) {:?}",self.my_pe,barrier_id,&[barrier_id].as_ptr());
                // self.bar_print("0".to_string());

                self.check_barrier_vals(barrier_id, &bufs.barrier2);
                barrier_id += 1;
                let barrier3_slice = unsafe { bufs.barrier3.as_mut_slice().unwrap() };
                barrier3_slice[0] = barrier_id;
                let barrier_slice = &[barrier_id];
                // self.bar_print("1".to_string());
                // println!("[{:?}] putting new barrier val ({:?}) {:?}",self.my_pe,barrier_id,barrier_slice.as_ptr());

                self.put_barrier_val(my_index, barrier_slice, &bufs.barrier1);
                // self.print_bar();
                // self.bar_print("2".to_string());
                // println!("[{:?}] checking new barrier val ({:?})",self.my_pe,barrier_id);
                self.check_barrier_vals(barrier_id, &bufs.barrier1);
                // self.bar_print("3".to_string());
                // println!("[{:?}] setting barrier exit val ({:?})",self.my_pe,barrier_id);
                barrier3_slice[1] = barrier_id;
                // self.bar_print("4".to_string());

                let barrier_slice = &barrier3_slice[1..2];

                self.put_barrier_val(my_index, barrier_slice, &bufs.barrier2);
            }
            // self.print_bar();
            // self.bar_print("5".to_string());
            // println!("[{:?}] checking barrier exit ({:?})",self.my_pe,barrier_id);
        }
    }
}

impl Drop for Barrier{
    fn drop(&mut self){
        //println!("dropping barrier");
        // println!("arch: {:?}",Arc::strong_count(&self.arch));
        //println!("dropped barrier");
    }
}

