/// ------------Lamellar Bandwidth: AM flops  -------------------------
/// similar to the AM bandwidth tests but instead of transferring data
/// each active message performs some number of dummy multiply add operations
/// Not to be used as a true measure of the flops of a system but can be
/// useful to compare multiple systems and/or perform worksize to
/// RT latency analyses
/// --------------------------------------------------------------------
use lamellar::ActiveMessaging;
use std::time::Instant;

// #[cfg(feature = "nightly")]
//use packed_simd::{f64x8, Simd};

#[lamellar::AmData(Clone, Debug)]
struct FlopAM {
    iterations: usize,
}

#[lamellar::am]
impl LamellarAM for FlopAM {
    fn exec(&self) -> usize {
        let mut a = [1.2345f64; 128];
        for _i in 0..self.iterations {
            for a_i in a.iter_mut() {
                *a_i = (*a_i) * (*a_i) + (*a_i);
            }
        }
        let temp_ops = self.iterations * a.len() * 2;
        let num_ops = if temp_ops > a.len() * 2 {
            temp_ops as usize
        } else {
            //this should never actually happen but should prevent optimizing A away
            a[0] as usize
        };
        num_ops
    }
}

// #[cfg(feature = "nightly")]
// #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
// struct SimdAM {
//     iterations: usize,
// }
// #[cfg(feature = "nightly")]
// #[lamellar::am]
// impl LamellarAM for SimdAM {
//     fn exec(&self) -> usize {
//         let mut a: [Simd<[f64; 8]>; 16] = [f64x8::new(
//             1.2345, 1.2345, 1.2345, 1.2345, 1.2345, 1.2345, 1.2345, 1.2345,
//         ); 16];
//         for _i in 0..self.iterations {
//             for a_i in a.iter_mut() {
//                 *a_i = (*a_i) * (*a_i) + (*a_i);
//             }
//         }
//         let temp_ops = self.iterations * a.len() * 8 * 2;
//         let num_ops = if temp_ops > a.len() * 2 {
//             temp_ops as usize
//         } else {
//             //this should never actually happen but should prevent optimizing A away
//             a[0].extract(1) as usize
//         };
//         num_ops
//     }
// }

fn main() {
    let world = lamellar::LamellarWorldBuilder::new()
        // .with_lamellae( Backend::Local )
        .build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();
    let s = Instant::now();
    world.barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);

    if my_pe == 0 {
        println!("==================Flop test===========================");
    }
    let mut flops = vec![];
    let num_tasks = 1000;
    let num_cores = match std::env::var("LAMELLAR_THREADS") {
        Ok(n) => n.parse::<usize>().unwrap() as f64,
        Err(_) => 4 as f64,
    };

    for i in 0..27 {
        let num_iterations = 2_u64.pow(i) as usize;

        let timer = Instant::now();

        let mut sub_time = 0f64;
        let mut reqs = vec![];
        if my_pe == 0 {
            for _j in 0..num_tasks {
                let sub_timer = Instant::now();
                reqs.push(world.exec_am_all(FlopAM {
                    iterations: num_iterations,
                }));

                sub_time += sub_timer.elapsed().as_secs_f64();
            }
            println!("issue time: {:?}", timer.elapsed().as_secs_f64());
            world.wait_all();
        }
        world.barrier();
        let cur_t = timer.elapsed().as_secs_f64();
        let tot_flop: usize = reqs
            .iter()
            .map(|r| r.get_all().drain(0..).map(|r| r.unwrap()).sum::<usize>())
            .sum();
        let task_granularity = ((cur_t * num_cores) / (num_tasks * num_pes) as f64) * 1000.0f64;
        if my_pe == 0 {
            println!(
                "iter size: {:?} tot_flop: {:?} time: {:?} (issue time: {:?})
                GFLOPS (avg): {:?} ({:?}%) task_gran: {:?}(ms)",
                num_iterations, //transfer size
                tot_flop,       //num transfers
                cur_t,          //transfer time
                sub_time,
                ((tot_flop as f64) / cur_t) / 1_000_000_000f64, // throughput of user payload
                (((tot_flop as f64) / cur_t) / 1_000_000_000f64) / 2800f64,
                task_granularity,
            );
            flops.push(((tot_flop as f64) / cur_t) / 1_000_000_000f64);
        }

        // #[cfg(feature = "nightly")]
        // {
        //     reqs.clear();
        //     let timer = Instant::now();
        //     let mut sub_time = 0f64;
        //     if my_pe == 0 {
        //         for _j in 0..num_tasks {
        //             let sub_timer = Instant::now();
        //             reqs.push(world.exec_am_all(SimdAM {
        //                 iterations: num_iterations,
        //             }));
        //             sub_time += sub_timer.elapsed().as_secs_f64();
        //         }
        //         println!("issue time: {:?}", timer.elapsed().as_secs_f64());
        //         world.wait_all();
        //     }

        //     world.barrier();
        //     let cur_t = timer.elapsed().as_secs_f64();
        //     let tot_flop: usize = reqs
        //         .iter()
        //         .map(|r| r.get_all().iter().map(|r| r.unwrap()).sum::<usize>())
        //         .sum();
        //     let task_granularity = ((cur_t * 24f64) / num_tasks as f64) * 1000.0f64;
        //     if my_pe == 0 {
        //         println!(
        //             "nightly iter size: {:?} tot_flop: {:?} time: {:?} (issue time: {:?})
        //             GFLOPS (avg): {:?} ({:?}%) task_gran: {:?}(ms)",
        //             num_iterations, //transfer size
        //             tot_flop,       //num transfers
        //             cur_t,          //transfer time
        //             sub_time,
        //             ((tot_flop as f64) / cur_t) / 1_000_000_000f64, // throughput of user payload
        //             (((tot_flop as f64) / cur_t) / 1_000_000_000f64) / 2800f64,
        //             task_granularity,
        //         );
        //         flops.push(((tot_flop as f64) / cur_t) / 1_000_000_000f64);
        //     }
        // }
    }
    if my_pe == 0 {
        println!(
            "flops: {}",
            flops
                .iter()
                .fold(String::new(), |acc, &num| acc + &num.to_string() + ", ")
        );
    }
}
