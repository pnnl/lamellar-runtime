/// ------------Lamellar Bandwidth: multi-threaded RDMA Put  -------------------------
/// Same measurement as put_bw, but the puts are issued from inside local AMs, one AM pinned to
/// each Lamellar worker thread, so RDMA is issued concurrently from every thread of PE 0.
/// Each thread writes a disjoint range of a different peer's shared memory region (set
/// BW_MT_SINGLE_DST=1 to send everything to the last PE instead).
/// --------------------------------------------------------------------
use lamellar::active_messaging::prelude::*;
use lamellar::memregion::prelude::*;
use std::time::Instant;

const ARRAY_LEN: usize = 2 * 1024 * 1024 * 1024;

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
    num_bytes: usize,
    count: usize,
}

#[lamellar::local_am]
impl LamellarAM for PutAm {
    async fn exec(self) -> usize {
        let mut sent = 0;
        for k in 0..self.count {
            unsafe {
                let _ = self.array.put_buffer_unmanaged(
                    self.dst_pe,
                    self.base + k * self.num_bytes,
                    self.data.sub_region(..self.num_bytes),
                );
            }
            sent += self.num_bytes;
        }
        sent
    }
}

#[lamellar::main]
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let num_threads = world.num_threads_per_pe();
    let single_dst = std::env::var("BW_MT_SINGLE_DST").is_ok();
    let array = world.alloc_shared_mem_region::<u8>(ARRAY_LEN).block();
    let data = world.alloc_one_sided_mem_region::<u8>(ARRAY_LEN);
    unsafe {
        for i in data.as_mut_slice() {
            *i = my_pe as u8;
        }
        for i in array.as_mut_slice() {
            *i = 255 as u8;
        }
    }
    // Only peers on a different host exercise the network: same-node transfers go through
    // shared memory and are not limited by the HCA. BW_MT_ANY_DST=1 includes same-node peers.
    let any_dst = std::env::var("BW_MT_ANY_DST").is_ok();
    let other_pes: Vec<usize> = if my_pe == 0 {
        let my_host = hostname();
        let hosts = world.exec_am_all(HostAm {}).block();
        (0..num_pes)
            .filter(|p| *p != my_pe && (any_dst || hosts[*p] != my_host))
            .collect()
    } else {
        vec![]
    };
    if my_pe == 0 {
        println!(
            "destinations: {} of {} peers ({})",
            other_pes.len(),
            num_pes - 1,
            if any_dst { "any host" } else { "remote hosts only" }
        );
    }

    world.barrier();
    let s = Instant::now();
    world.barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);

    if my_pe == 0 {
        println!(
            "==================Bandwidth test (multi-threaded: {} threads, {})===========================",
            num_threads,
            if single_dst { "single dst" } else { "spread dsts" }
        );
    }
    let mut bws = vec![];
    for i in 0..30 {
        let num_bytes = 2_u64.pow(i);
        let mut sum = 0u64;
        let mut cnt = 0u64;
        let mut exp = 20;
        if num_bytes <= 2048 {
            exp = 18 + i;
        } else if num_bytes >= 4096 {
            exp = 30;
        }
        let total_tx = (2_u64.pow(exp) / num_bytes).max(1) as usize;
        let timer = Instant::now();
        if my_pe == 0 && !other_pes.is_empty() {
            let active = num_threads.min(total_tx).max(1);
            let per_thread = total_tx / active;
            let team = world.team();
            let world_c = world.clone();
            let array_c = array.clone();
            let data_c = data.clone();
            let others = other_pes.clone();
            let sent: Vec<usize> = world.block_on(async move {
                let handles: Vec<_> = (0..active)
                    .map(|t| {
                        let dst_pe = if single_dst {
                            *others.last().unwrap()
                        } else {
                            others[t % others.len()]
                        };
                        team.exec_am_local_thread(
                            PutAm {
                                array: array_c.clone(),
                                data: data_c.clone(),
                                dst_pe,
                                base: t * per_thread * num_bytes as usize,
                                num_bytes: num_bytes as usize,
                                count: per_thread,
                            },
                            t,
                        )
                        .spawn()
                    })
                    .collect();
                world_c.join_all(handles).await
            });
            println!("issue time: {:?}", timer.elapsed());
            for s in sent {
                sum += s as u64;
            }
            cnt = (active * per_thread) as u64;
            array.wait_all();
        }
        world.barrier();
        let cur_t = timer.elapsed().as_secs_f64();
        if my_pe == 0 {
            println!(
                "tx_size: {:?}B num_tx: {:?} num_bytes: {:?}MB time: {:?} throughput (avg): {:?}MB/s (cuml): {:?}MB/s latency: {:?}us",
                num_bytes,
                cnt,
                sum as f64 / 1048576.0,
                cur_t,
                (sum as f64 / 1048576.0) / cur_t,
                (sum as f64 / 1048576.0) / cur_t,
                (cur_t / cnt.max(1) as f64) * 1_000_000 as f64,
            );
        }
        bws.push((sum as f64 / 1048576.0) / cur_t);
        unsafe {
            for i in array.as_mut_slice() {
                *i = 255 as u8;
            }
        };
        world.barrier();
    }
    if my_pe == 0 {
        println!(
            "bandwidths: {}",
            bws.iter()
                .fold(String::new(), |acc, &num| acc + &num.to_string() + ", ")
        );
    }
    world.barrier();
}
