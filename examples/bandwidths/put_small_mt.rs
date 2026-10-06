/// ------------Lamellar micro-benchmark: multi-threaded small RDMA puts -------------------------
/// PE 0 runs one local AM pinned to each worker thread; every AM issues PUT_ITERS unmanaged puts of
/// PUT_SIZE bytes to a remote-host peer. Prints puts/s per round. Env: PUT_SIZE (8), PUT_ITERS
/// (20000 per thread), PUT_ROUNDS (3), PUT_SINGLE_DST=1 (all threads -> one peer),
/// PUT_PRIVATE_SRC=1 (each thread uses its own source region instead of sharing one).
/// --------------------------------------------------------------------
use lamellar::active_messaging::prelude::*;
use lamellar::memregion::prelude::*;
use std::time::Instant;

#[lamellar::AmData(Clone, Debug)]
struct HostAm;

#[lamellar::am]
impl LamellarAM for HostAm {
    async fn exec(self) -> String {
        hostname()
    }
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .unwrap_or_default()
        .trim()
        .to_string()
}

#[lamellar::AmLocalData(Clone)]
struct PutAm {
    array: SharedMemoryRegion<u8>,
    data: OneSidedMemoryRegion<u8>,
    dst_pe: usize,
    base: usize,
    size: usize,
    iters: usize,
}

#[lamellar::local_am]
impl LamellarAM for PutAm {
    async fn exec(self) -> usize {
        for k in 0..self.iters {
            unsafe {
                let _ = self.array.put_buffer_unmanaged(
                    self.dst_pe,
                    self.base + (k % 1024) * self.size,
                    self.data.sub_region(..self.size),
                );
            }
        }
        self.iters
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let num_threads = world.num_threads_per_pe();
    let size = env_usize("PUT_SIZE", 8);
    let iters = env_usize("PUT_ITERS", 20000);
    let rounds = env_usize("PUT_ROUNDS", 3);
    let single = std::env::var("PUT_SINGLE_DST").is_ok();
    let span = 1024 * size * num_threads + 4096;
    let array = world.alloc_shared_mem_region::<u8>(span).block();
    let data = world.alloc_one_sided_mem_region::<u8>(size.max(64));
    unsafe {
        for i in data.as_mut_slice() {
            *i = my_pe as u8;
        }
        for i in array.as_mut_slice() {
            *i = 255 as u8;
        }
    }
    world.barrier();
    let remote: Vec<usize> = if my_pe == 0 {
        let my_host = hostname();
        let hosts = world.exec_am_all(HostAm {}).block();
        (0..num_pes).filter(|p| *p != 0 && hosts[*p] != my_host).collect()
    } else {
        vec![]
    };
    if my_pe == 0 {
        println!(
            "put_small_mt: size={}B iters/thread={} threads={} remote_peers={} single_dst={}",
            size, iters, num_threads, remote.len(), single
        );
    }
    for round in 0..rounds {
        world.barrier();
        if my_pe == 0 && !remote.is_empty() {
            let team = world.team();
            let world_c = world.clone();
            let array_c = array.clone();
            let private = std::env::var("PUT_PRIVATE_SRC").is_ok();
            // one source region per thread when PUT_PRIVATE_SRC is set: no shared refcount
            let datas: Vec<OneSidedMemoryRegion<u8>> = (0..num_threads)
                .map(|_| {
                    if private {
                        let d = world.alloc_one_sided_mem_region::<u8>(size.max(64));
                        unsafe { for i in d.as_mut_slice() { *i = my_pe as u8; } }
                        d
                    } else {
                        data.clone()
                    }
                })
                .collect();
            let remote_c = remote.clone();
            let t0 = Instant::now();
            let done: Vec<usize> = world.block_on(async move {
                let handles: Vec<_> = (0..num_threads)
                    .map(|t| {
                        let dst_pe = if single { remote_c[0] } else { remote_c[t % remote_c.len()] };
                        team.exec_am_local_thread(
                            PutAm {
                                array: array_c.clone(),
                                data: datas[t].clone(),
                                dst_pe,
                                base: t * 1024 * size,
                                size,
                                iters,
                            },
                            t,
                        )
                        .spawn()
                    })
                    .collect();
                world_c.join_all(handles).await
            });
            let issue = t0.elapsed().as_secs_f64();
            array.wait_all();
            let total = t0.elapsed().as_secs_f64();
            let n: usize = done.iter().sum();
            println!(
                "round {}: {} puts  issue {:.3}s ({:.3} Mputs/s)  with wait_all {:.3}s ({:.3} Mputs/s, {:.1} MB/s)",
                round,
                n,
                issue,
                n as f64 / issue / 1e6,
                total,
                n as f64 / total / 1e6,
                (n * size) as f64 / total / 1e6,
            );
        }
        world.barrier();
    }
    world.barrier();
}
