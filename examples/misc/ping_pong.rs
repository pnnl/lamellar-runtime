use std::sync::Arc;

use lamellar::active_messaging::prelude::*;
use lamellar::memregion::prelude::*;
// use parking_lot::Mutex;
use async_lock::{Mutex, Semaphore};
use rand::prelude::*;
use std::collections::VecDeque;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

const UPDATES_PER_CORE: usize = 10_000_000;

#[derive(Clone)]
struct IdxAmBuffer {
    idx_send_buffer: SharedMemoryRegion<usize>,
    idx_recv_buffer: SharedMemoryRegion<usize>,
    res_recv_buffer: SharedMemoryRegion<usize>,
}

#[derive(Clone)]
struct ResAmBuffer {
    idx_recv_buffer: SharedMemoryRegion<usize>,
    res_send_buffer: SharedMemoryRegion<usize>,
    res_recv_buffer: SharedMemoryRegion<usize>,
}

#[AmLocalData]
struct RecvAm {
    buffer: ResAmBuffer,
    remote_pe: usize,
    buffer_size: usize,
    finished: Arc<AtomicU8>,
}

#[local_am]
impl LamellarAm for RecvAm {
    async fn exec(self) {
        unsafe {
            let _cnt = 0;

            let start = self.remote_pe * self.buffer_size;
            let end = start + self.buffer_size;
            let my_start = lamellar::current_pe * self.buffer_size;
            let res_send_buf = self.buffer.res_send_buffer.sub_region(start..end);
            let idx_recv_buf = self.buffer.idx_recv_buffer.sub_region(start..end);
            let idx_recv_slice = idx_recv_buf.as_mut_slice();

            'outer: while self.finished.load(Ordering::SeqCst) == 0 {
                while idx_recv_slice[0] == usize::MAX {
                    if self.finished.load(Ordering::SeqCst) != 0 {
                        break 'outer;
                    }
                    async_std::task::yield_now().await;
                }

                idx_recv_slice[0] = usize::MAX;
                self.buffer
                    .res_recv_buffer
                    .put_buffer(self.remote_pe, my_start, res_send_buf.clone())
                    .await;
            }
        }
    }
}

#[AmLocalData]
struct SendAm {
    indices: Vec<usize>,
    buffers: Arc<Vec<Mutex<VecDeque<IdxAmBuffer>>>>,
    remote_pe: usize,
    buffer_size: usize,
    comm_lock: Arc<Semaphore>,
}

#[local_am]
impl LamellarAm for SendAm {
    async fn exec(self) {
        let mut buffers: Option<IdxAmBuffer> = None;

        {
            while buffers.is_none() {
                buffers = self.buffers[self.remote_pe].lock().await.pop_front();
                async_std::task::yield_now().await;
            }
        }
        let buffer = buffers.unwrap();
        let start = self.remote_pe * self.buffer_size;
        let end = start + self.indices.len();
        let my_start = lamellar::current_pe * self.buffer_size;

        unsafe {
            std::ptr::copy_nonoverlapping(
                self.indices.as_ptr(),
                buffer
                    .idx_send_buffer
                    .sub_region(start..end)
                    .as_mut_ptr()
                    .unwrap(),
                self.indices.len(),
            );
            let _comm = self.comm_lock.acquire().await;
            buffer
                .idx_recv_buffer
                .put_buffer(
                    self.remote_pe,
                    my_start,
                    buffer.idx_send_buffer.sub_region(start..end),
                )
                .await;

            let sub_reg = buffer.res_recv_buffer.sub_region(start..end);

            let mut t_start = std::time::Instant::now();

            while sub_reg.as_mut_slice()[0] == usize::MAX {
                if t_start.elapsed().as_secs() > 5 {
                    println!(
                        "[pe:{:?} send_am tid:{:?},{:?}] timeout waiting for response from: {} at {} {:?} len: {}",
                        lamellar::current_pe,
                        lamellar::tid,
                        std::thread::current().id(),
                        self.remote_pe,
                        my_start,
                        &sub_reg.as_mut_slice()[0..5],
                        self.indices.len()
                    );
                    t_start = std::time::Instant::now();
                }
                async_std::task::yield_now().await;
            }
            buffer.res_recv_buffer.sub_region(start..end).as_mut_slice()[0] = usize::MAX;

            self.buffers[self.remote_pe].lock().await.push_back(buffer);
        }
    }
}

#[AmLocalData]
struct MyAm {
    indices: SharedMemoryRegion<usize>,
    buffers: Arc<Vec<Mutex<VecDeque<IdxAmBuffer>>>>,
    buffer_size: usize,
    table_size_per_pe: usize,
    comm_lock: Arc<Semaphore>,
}

#[local_am]
impl LamellarAm for MyAm {
    async fn exec(self) {
        let indices_slice = unsafe { self.indices.as_mut_slice() };

        println!("my_am: {:?} {:?}", indices_slice.len(), self.buffer_size);

        let num_pes = lamellar::num_pes;
        let mut pe_bufs = vec![vec![]; num_pes];
        let timer = std::time::Instant::now();
        let mut cnt = 0;
        let task_group = LamellarTaskGroup::new(lamellar::team.clone());
        for i in indices_slice.iter() {
            let pe = i / self.table_size_per_pe;
            let offset = lamellar::current_pe; //i % self.table_size_per_pe;
            pe_bufs[pe].push(offset);
            if pe_bufs[pe].len() >= self.buffer_size {
                let mut indices = vec![];
                std::mem::swap(&mut indices, &mut pe_bufs[pe]);
                let _ = task_group
                    .exec_am_local(SendAm {
                        indices,
                        buffers: self.buffers.clone(),
                        remote_pe: pe,
                        buffer_size: self.buffer_size,
                        comm_lock: self.comm_lock.clone(),
                    })
                    .spawn();
                cnt += 1;
            }
        }
        for (pe, indices) in pe_bufs.drain(..).enumerate() {
            if indices.len() > 0 {
                let _ = task_group
                    .exec_am_local(SendAm {
                        indices,
                        buffers: self.buffers.clone(),
                        remote_pe: pe,
                        buffer_size: self.buffer_size,
                        comm_lock: self.comm_lock.clone(),
                    })
                    .spawn();
                cnt += 1;
            }
        }
        println!("launch time {:?} {:?}", cnt, timer.elapsed());
        task_group.await_all().await;
        println!(
            "[pe:{:?}] my_am finished at {} cnt: {} time:{:?} ",
            lamellar::current_pe,
            lamellar::current_pe * self.buffer_size / self.buffer_size,
            cnt,
            timer.elapsed()
        );
    }
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let buffer_size = 16384 * 2;

    let indices = world
        .alloc_shared_mem_region::<usize>(UPDATES_PER_CORE * world.num_threads_per_pe())
        .block();

    let index_send_buffers = world
        .alloc_shared_mem_region::<usize>(buffer_size * num_pes)
        .block();
    world.barrier();
    let index_recv_buffers = world
        .alloc_shared_mem_region::<usize>(buffer_size * num_pes)
        .block();
    world.barrier();
    let result_send_buffers = world
        .alloc_shared_mem_region::<usize>(buffer_size * num_pes)
        .block();
    world.barrier();
    let result_recv_buffers = world
        .alloc_shared_mem_region::<usize>(buffer_size * num_pes)
        .block();
    world.barrier();
    let mut rng: StdRng = SeedableRng::seed_from_u64(my_pe as u64);
    let table_size_per_pe = 100000 * world.num_threads_per_pe();
    let global_size = table_size_per_pe * num_pes;

    unsafe {
        index_send_buffers
            .as_mut_slice()
            .iter_mut()
            .for_each(|x| *x = usize::MAX);
        index_recv_buffers
            .as_mut_slice()
            .iter_mut()
            .for_each(|x| *x = usize::MAX);
        result_send_buffers
            .as_mut_slice()
            .iter_mut()
            .for_each(|x| *x = my_pe);
        result_recv_buffers
            .as_mut_slice()
            .iter_mut()
            .for_each(|x| *x = usize::MAX);
        indices
            .as_mut_slice()
            .iter_mut()
            .for_each(|x| *x = rng.random_range(0..global_size));
    }
    world.barrier();

    let mut res_am_buffers = Vec::new();
    let mut send_am_buffers = Vec::new();

    for _i in 0..num_pes {
        let mut pe_buffer = VecDeque::new();
        let idx_buffers = IdxAmBuffer {
            idx_send_buffer: index_send_buffers.clone(),
            idx_recv_buffer: index_recv_buffers.clone(),
            res_recv_buffer: result_recv_buffers.clone(),
        };
        pe_buffer.push_back(idx_buffers);
        send_am_buffers.push(Mutex::new(pe_buffer));

        let res_buffers = ResAmBuffer {
            idx_recv_buffer: index_recv_buffers.clone(),
            res_send_buffer: result_send_buffers.clone(),
            res_recv_buffer: result_recv_buffers.clone(),
        };
        res_am_buffers.push(res_buffers.clone());
    }
    let buffers = Arc::new(send_am_buffers);
    let finished = Arc::new(AtomicU8::new(0));
    let comm_lock = Arc::new(Semaphore::new(1024));
    world.barrier();
    let timer = std::time::Instant::now();
    for (pe, buffer) in res_am_buffers.iter().enumerate() {
        let _ = world.spawn_am_local(RecvAm {
            buffer: buffer.clone(),
            remote_pe: pe,
            finished: finished.clone(),
            buffer_size,
        });
    }
    let mut reqs = vec![];
    // if my_pe == 0 {
    for _thread in 0..1 {
        //world.num_threads_per_pe() {
        reqs.push(world.spawn_am_local(MyAm {
            indices: indices.clone(),
            buffers: buffers.clone(),
            buffer_size,
            table_size_per_pe: table_size_per_pe,
            comm_lock: comm_lock.clone(),
        }));
    }
    world.block_on_all(reqs);
    // }
    world.barrier();
    println!(
        "time {:?} {:?} total updates MUPS: {:?}",
        timer.elapsed(),
        indices.len() * num_pes,
        ((indices.len() * num_pes) as f64 / 1000000.0) / timer.elapsed().as_secs_f64()
    );
    finished.store(1, Ordering::SeqCst);
}
