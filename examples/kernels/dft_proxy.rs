// use futures_util::FutureExt;
use futures_util::StreamExt;
use lamellar::active_messaging::prelude::*;
/// ------------Lamellar Bandwidth: DFT Proxy  -------------------------
/// This example is inspired from peforming a naive DFT
/// it does not actually calculate a DFT as we simply perform the transform
/// using f64s, rather than using complex representations.
/// we include the distributed Lamellar Implemtation
/// as well as a (single process) shared memory version using Rayon.
/// --------------------------------------------------------------------
use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::*;
use std::time::Instant;
#[macro_use]
extern crate lazy_static;

const MAGIC: f64 = std::f64::MAX;

lazy_static! {
    static ref LOCK: Mutex<()> = Mutex::new(());
}

#[lamellar::AmData(Clone, Debug)]
struct ReduceAM {
    spectrum: SharedMemoryRegion<f64>,
}

#[lamellar::am]
impl LamellarAM for ReduceAM {
    async fn exec(self) -> f64 {
        unsafe { self.spectrum.as_slice().iter().sum::<f64>() }
    }
}

#[lamellar::AmData(Clone, Debug)]
struct LocalSumAM {
    spectrum: SharedMemoryRegion<f64>,
    signal: SharedMemoryRegion<f64>,
    global_sig_len: usize,
    k: usize,
    pe: usize,
}

#[lamellar::local_am]
impl LamellarAM for LocalSumAM {
    async fn exec() {
        let spectrum_slice = unsafe { self.spectrum.as_mut_slice() };
        let k_prime = self.k + self.pe * spectrum_slice.len();
        let signal = unsafe { self.signal.as_slice() };
        let mut sum = 0.0;
        for (i, &x) in signal.iter().enumerate() {
            let i_prime = i + lamellar::current_pe as usize * signal.len();
            let angle = -1f64 * (i_prime * k_prime) as f64 * 2f64 * std::f64::consts::PI
                / self.global_sig_len as f64;
            let twiddle = angle * (angle.cos() + angle * angle.sin());
            sum = sum + twiddle * x;
        }
        let _lock = LOCK.lock();
        spectrum_slice[self.k] = sum;
    }
}

#[lamellar::AmData(Clone, Debug)]
struct LocalSumAM2 {
    local_len: usize,
    signal: SharedMemoryRegion<f64>,
    global_sig_len: usize,
    k: usize,
    pe: usize,
}

#[lamellar::am]
impl LamellarAM for LocalSumAM2 {
    async fn exec() -> f64 {
        let k_prime = self.k + self.pe * self.local_len;
        let signal = unsafe { self.signal.as_slice() };
        let mut sum = 0.0;
        for (i, &x) in signal.iter().enumerate() {
            let i_prime = i + lamellar::current_pe as usize * signal.len();
            let angle = -1f64 * (i_prime * k_prime) as f64 * 2f64 * std::f64::consts::PI
                / self.global_sig_len as f64;
            let twiddle = angle * (angle.cos() + angle * angle.sin());
            sum = sum + twiddle * x;
        }
        sum
    }
}

#[lamellar::AmData(Clone, Debug)]
struct LocalSumAM2Static {
    local_len: usize,
    #[AmGroup(static)]
    signal: SharedMemoryRegion<f64>,
    global_sig_len: usize,
    k: usize,
    pe: usize,
}

#[lamellar::am]
impl LamellarAM for LocalSumAM2Static {
    async fn exec() -> f64 {
        let k_prime = self.k + self.pe * self.local_len;
        let signal = unsafe { self.signal.as_slice() };
        let mut sum = 0.0;
        for (i, &x) in signal.iter().enumerate() {
            let i_prime = i + lamellar::current_pe as usize * signal.len();
            let angle = -1f64 * (i_prime * k_prime) as f64 * 2f64 * std::f64::consts::PI
                / self.global_sig_len as f64;
            let twiddle = angle * (angle.cos() + angle * angle.sin());
            sum = sum + twiddle * x;
        }
        sum
    }
}

#[lamellar::AmData(Clone, Debug)]
struct RemoteSumAM {
    spectrum: SharedMemoryRegion<f64>,
    add_spec: Vec<f64>,
}

#[lamellar::am]
impl LamellarAM for RemoteSumAM {
    async fn exec(self) {
        let _lock = LOCK.lock();
        for (k, spec_bin) in unsafe { self.spectrum.as_mut_slice().iter_mut().enumerate() } {
            *spec_bin += self.add_spec[k];
        }
    }
}

fn dft_lamellar(
    world: &LamellarWorld,
    _my_pe: usize,
    num_pes: usize,
    signal: SharedMemoryRegion<f64>,
    global_sig_len: usize,
    spectrum: SharedMemoryRegion<f64>,
) -> f64 {
    let spectrum_slice = unsafe { spectrum.as_slice() };
    let add_spec = world
        .alloc_shared_mem_region::<f64>(spectrum_slice.len())
        .block();

    let timer = Instant::now();
    for pe in 0..num_pes {
        for k in 0..spectrum_slice.len() {
            let _ = world
                .exec_am_local(LocalSumAM {
                    spectrum: add_spec.clone(),
                    signal: signal.clone(),
                    global_sig_len: global_sig_len,
                    k: k,
                    pe: pe,
                })
                .spawn();
        }
        let mut add_spec_vec = vec![0.0; spectrum_slice.len()];
        world.wait_all();
        add_spec_vec.copy_from_slice(unsafe { add_spec.as_slice() });
        let _ = world
            .exec_am_pe(
                pe,
                RemoteSumAM {
                    spectrum: spectrum.clone(),
                    add_spec: add_spec_vec,
                },
            )
            .spawn();
        world.wait_all();
    }
    world.wait_all();
    world.barrier();
    let time = timer.elapsed().as_secs_f64();
    time
}

fn dft_lamellar_am_group(
    world: &LamellarWorld,
    my_pe: usize,
    num_pes: usize,
    signal: SharedMemoryRegion<f64>,
    global_sig_len: usize,
    spectrum: SharedMemoryRegion<f64>,
) -> f64 {
    let spectrum_slice = unsafe { spectrum.as_slice() };
    let local_len = spectrum_slice.len();

    let timer = Instant::now();

    let mut pe_groups = futures_util::stream::FuturesOrdered::new();
    for pe in 0..num_pes {
        let mut local_sum_group = typed_am_group!(LocalSumAM2, world);
        for k in 0..local_len {
            local_sum_group.add_am_pe(
                my_pe,
                LocalSumAM2 {
                    local_len: local_len,
                    signal: signal.clone(),
                    global_sig_len: global_sig_len,
                    k: k,
                    pe: pe,
                },
            );
        }
        let spec = spectrum.clone();
        let world_clone = world.clone();
        pe_groups.push_back(async move {
            let res = local_sum_group.exec().await;
            let vec = (0..local_len)
                .map(|i| {
                    if let AmGroupResult::Pe(_, val) = res.at(i) {
                        *val
                    } else {
                        panic!("cant reach")
                    }
                })
                .collect::<Vec<_>>();
            world_clone
                .exec_am_pe(
                    pe,
                    RemoteSumAM {
                        spectrum: spec,
                        add_spec: vec,
                    },
                )
                .await;
        });
    }
    world.block_on(async move { pe_groups.collect::<Vec<_>>().await });

    world.barrier();
    let time = timer.elapsed().as_secs_f64();
    time
}

fn dft_lamellar_am_group_static(
    world: &LamellarWorld,
    my_pe: usize,
    num_pes: usize,
    signal: SharedMemoryRegion<f64>,
    global_sig_len: usize,
    spectrum: SharedMemoryRegion<f64>,
) -> f64 {
    let spectrum_slice = unsafe { spectrum.as_slice() };
    let local_len = spectrum_slice.len();

    let timer = Instant::now();

    let mut pe_groups = futures_util::stream::FuturesOrdered::new();
    for pe in 0..num_pes {
        let mut local_sum_group = typed_am_group!(LocalSumAM2Static, world);
        for k in 0..local_len {
            local_sum_group.add_am_pe(
                my_pe,
                LocalSumAM2Static {
                    local_len: local_len,
                    signal: signal.clone(),
                    global_sig_len: global_sig_len,
                    k: k,
                    pe: pe,
                },
            );
        }
        let spec = spectrum.clone();
        let world_clone = world.clone();
        pe_groups.push_back(async move {
            let res = local_sum_group.exec().await;
            let vec = (0..local_len)
                .map(|i| {
                    if let AmGroupResult::Pe(_, val) = res.at(i) {
                        *val
                    } else {
                        panic!("cant reach")
                    }
                })
                .collect::<Vec<_>>();
            world_clone
                .exec_am_pe(
                    pe,
                    RemoteSumAM {
                        spectrum: spec,
                        add_spec: vec,
                    },
                )
                .await;
        });
    }
    world.block_on(async move {
        pe_groups.collect::<Vec<_>>().await;
    });

    world.barrier();
    let time = timer.elapsed().as_secs_f64();
    time
}

fn dft_serial(signal: &[f64], spectrum: &mut [f64]) -> f64 {
    let timer = Instant::now();
    for (k, spec_bin) in spectrum.iter_mut().enumerate() {
        let mut sum = 0f64;
        for (i, &x) in signal.iter().enumerate() {
            let angle = -1f64 * (i * k) as f64 * 2f64 * std::f64::consts::PI / signal.len() as f64;
            let twiddle = angle * (angle.cos() + angle * angle.sin());
            sum = sum + twiddle * x;
        }
        *spec_bin = sum;
    }
    timer.elapsed().as_secs_f64()
}

fn dft_rayon(signal: &[f64], spectrum: &mut [f64]) -> f64 {
    let timer = Instant::now();
    spectrum
        .par_iter_mut()
        .enumerate()
        .for_each(|(k, spec_bin)| {
            let mut sum = 0f64;
            for (i, &x) in signal.iter().enumerate() {
                let angle =
                    -1f64 * (i * k) as f64 * 2f64 * std::f64::consts::PI / signal.len() as f64;
                let twiddle = angle * (angle.cos() + angle * angle.sin());
                sum = sum + twiddle * x;
            }
            *spec_bin = sum
        });
    timer.elapsed().as_secs_f64()
}

// the easiest implementation using lamellar arrays, although not terribly performant
// because each iteration of the outer loop is transferring the entirety of the signal array
// without an reuse, using a buffered_onesided_iter helps to ensure the transfers are efficient, but
// a lot of (needless) data transfer happens
#[allow(dead_code)]
fn dft_lamellar_array(signal: UnsafeArray<f64>, spectrum: UnsafeArray<f64>) -> f64 {
    let timer = Instant::now();
    let signal_clone = signal.clone();
    unsafe {
        spectrum
            .dist_iter_mut()
            .enumerate()
            .for_each(move |(k, spec_bin)| {
                let mut sum = 0f64;
                for (i, &x) in signal_clone
                    .buffered_onesided_iter(1000)
                    .into_iter()
                    .enumerate()
                {
                    let angle = -1f64 * (i * k) as f64 * 2f64 * std::f64::consts::PI
                        / signal_clone.len() as f64;
                    let twiddle = angle * (angle.cos() + angle * angle.sin());
                    sum = sum + twiddle * x;
                }
                *spec_bin = sum
            })
            .block();
    }
    spectrum.barrier();
    timer.elapsed().as_secs_f64()
}

// the easiest implementation using lamellar arrays, although not terribly performance
// because each iteration of the outer loop is transferring the entirety of the signal array
// without any reuse, using a buffered_onesided_iter helps to ensure the transfers are efficient, but
// a lot of (needless) data transfer happens
#[allow(dead_code)]
fn dft_lamellar_array_2(signal: ReadOnlyArray<f64>, spectrum: AtomicArray<f64>) -> f64 {
    let timer = Instant::now();
    let signal_clone = signal.clone();
    let _ = spectrum
        .dist_iter_mut()
        .enumerate()
        .for_each(move |(k, spec_bin)| {
            let mut sum = 0f64;
            for (i, &x) in signal_clone
                .buffered_onesided_iter(1000)
                .into_iter()
                .enumerate()
            {
                let angle = -1f64 * (i * k) as f64 * 2f64 * std::f64::consts::PI
                    / signal_clone.len() as f64;
                let twiddle = angle * (angle.cos() + angle * angle.sin());
                sum = sum + twiddle * x;
            }
            spec_bin.store(sum);
        })
        .block();
    spectrum.barrier();
    timer.elapsed().as_secs_f64()
}

#[allow(dead_code)]
fn dft_lamellar_array_swapped(signal: UnsafeArray<f64>, spectrum: UnsafeArray<f64>) -> f64 {
    let timer = Instant::now();
    let signal_len = signal.len();

    unsafe {
        for (i, x) in signal.onesided_iter().into_iter().enumerate() {
            let x = (*x).clone();
            let _ = spectrum
                .dist_iter_mut()
                .enumerate()
                .for_each(move |(k, spec_bin)| {
                    let angle =
                        -1f64 * (i * k) as f64 * 2f64 * std::f64::consts::PI / signal_len as f64;
                    let twiddle = angle * (angle.cos() + angle * angle.sin());
                    let _lock = LOCK.lock();
                    *spec_bin += twiddle * x;
                })
                .spawn();
        }
    };
    spectrum.wait_all();
    spectrum.barrier();
    timer.elapsed().as_secs_f64()
}

// a more optimized version using lamellar arrays, and behaves similar to the manual active message implementation above
// here we create "copied chunks" of the signal array, which are then passed and reused during each iteration of the spectrum loop.
// in this case we only completely transfer the signal array one time
fn dft_lamellar_array_opt(
    signal: UnsafeArray<f64>,
    spectrum: UnsafeArray<f64>,
    buf_size: usize,
) -> f64 {
    let timer = Instant::now();
    let sig_len = signal.len();
    unsafe {
        signal
            .onesided_iter()
            .chunks(buf_size)
            // .buffered(2)
            .into_iter()
            .enumerate()
            .for_each(|(i, chunk)| {
                let signal = chunk.clone();

                let _ = spectrum
                    .dist_iter_mut()
                    .enumerate()
                    .for_each(move |(k, spec_bin)| {
                        let mut sum = 0f64;
                        for (j, &x) in signal
                            .iter()
                            .enumerate()
                            .map(|(j, x)| (j + i * buf_size, x))
                        {
                            let angle = -1f64 * (j * k) as f64 * 2f64 * std::f64::consts::PI
                                / sig_len as f64;
                            let twiddle = angle * (angle.cos() + angle * angle.sin());
                            sum = sum + twiddle * x;
                        }

                        // let _lock = LOCK.lock();
                        *spec_bin += sum;
                    })
                    .spawn();
            });
    }
    spectrum.wait_all();
    spectrum.barrier();
    timer.elapsed().as_secs_f64()
}

fn dft_lamellar_array_opt_test(
    signal: UnsafeArray<f64>,
    spectrum: UnsafeArray<f64>,
    buf_size: usize,
) -> f64 {
    let timer = Instant::now();
    let sig_len = signal.len();
    unsafe {
        signal
            .onesided_iter()
            .chunks(buf_size)
            // .buffered(2)
            .into_iter()
            .enumerate()
            .for_each(|(i, chunk)| {
                let signal = chunk.clone();
                let _ = spectrum
                    .dist_iter_mut()
                    .enumerate()
                    .for_each_with_schedule(Schedule::Dynamic, move |(k, spec_bin)| {
                        let mut sum = 0f64;
                        for (j, &x) in signal
                            .iter()
                            .enumerate()
                            .map(|(j, x)| (j + i * buf_size, x))
                        {
                            let angle = -1f64 * (j * k) as f64 * 2f64 * std::f64::consts::PI
                                / sig_len as f64;
                            let twiddle = angle * (angle.cos() + angle * angle.sin());
                            sum = sum + twiddle * x;
                        }

                        // let _lock = LOCK.lock();
                        *spec_bin += sum;
                    })
                    .spawn();
            });
    }
    spectrum.wait_all();
    spectrum.barrier();
    timer.elapsed().as_secs_f64()
}

// same as above but uses safe atomic arrays
fn dft_lamellar_array_opt_2(
    signal: ReadOnlyArray<f64>,
    spectrum: AtomicArray<f64>,
    buf_size: usize,
) -> f64 {
    let timer = Instant::now();
    let sig_len = signal.len();
    signal
        .onesided_iter()
        .chunks(buf_size)
        // .buffered(2)
        .into_iter()
        .enumerate()
        .for_each(|(i, chunk)| {
            let signal = chunk.clone();

            let _ = spectrum
                .dist_iter_mut()
                .enumerate()
                .for_each(move |(k, mut spec_bin)| {
                    let mut sum = 0f64;
                    unsafe {
                        for (j, &x) in signal
                            .iter()
                            .enumerate()
                            .map(|(j, x)| (j + i * buf_size, x))
                        {
                            let angle = -1f64 * (j * k) as f64 * 2f64 * std::f64::consts::PI
                                / sig_len as f64;
                            let twiddle = angle * (angle.cos() + angle * angle.sin());
                            sum = sum + twiddle * x;
                        }
                    }
                    spec_bin += sum;
                })
                .spawn();
        });
    spectrum.wait_all();
    spectrum.barrier();
    timer.elapsed().as_secs_f64()
}

// same as above but uses safe collective atomic arrays
fn dft_lamellar_array_opt_3(
    signal: ReadOnlyArray<f64>,
    spectrum: LocalLockArray<f64>,
    buf_size: usize,
) -> f64 {
    let timer = Instant::now();
    let sig_len = signal.len();
    signal
        .onesided_iter()
        .chunks(buf_size)
        // .buffered(2)
        .into_iter()
        .enumerate()
        .for_each(|(i, chunk)| {
            let signal = chunk.clone();

            let _ = spectrum
                .dist_iter_mut() //this locks the LocalLockArray
                .enumerate()
                .for_each(move |(k, spec_bin)| {
                    //we are accessing each element independently so free to mutate
                    let mut sum = 0f64;
                    unsafe {
                        for (j, &x) in signal
                            .iter()
                            .enumerate()
                            .map(|(j, x)| (j + i * buf_size, x))
                        {
                            let angle = -1f64 * (j * k) as f64 * 2f64 * std::f64::consts::PI
                                / sig_len as f64;
                            let twiddle = angle * (angle.cos() + angle * angle.sin());
                            sum = sum + twiddle * x;
                        }
                    }
                    *spec_bin += sum;
                })
                .spawn();
        });
    spectrum.wait_all();
    spectrum.barrier();
    timer.elapsed().as_secs_f64()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let array_len = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let buf_amt = args
        .get(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let num_trials = args
        .get(3)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 10);

    // let run_single_node = args
    //     .get(3)
    //     .and_then(|s| {
    //         if s == "--run-local" {
    //             Some(true)
    //         } else {
    //             Some(false)
    //         }
    //     })
    //     .unwrap_or_else(|| false);
    let run_single_node = false;

    // let mut rng = StdRng::seed_from_u64(10);
    let mut rng: StdRng = SeedableRng::seed_from_u64(10);

    if !run_single_node {
        let world = lamellar::LamellarWorldBuilder::new().build();
        let my_pe = world.my_pe();
        let num_pes = world.num_pes();
        let global_len = num_pes * array_len;

        println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);
        let partial_sum = world.alloc_shared_mem_region::<f64>(num_pes).block();
        let partial_spectrum = world.alloc_shared_mem_region::<f64>(array_len).block();
        let partial_signal = world.alloc_shared_mem_region::<f64>(array_len).block();
        let full_signal = world.alloc_one_sided_mem_region::<f64>(global_len);
        let full_spectrum = world.alloc_one_sided_mem_region::<f64>(global_len);
        let magic = world.alloc_one_sided_mem_region::<f64>(num_pes);

        let full_spectrum_array =
            UnsafeArray::<f64>::new(world.team(), global_len, Distribution::Block).block();
        let full_signal_array =
            UnsafeArray::<f64>::new(world.team(), global_len, Distribution::Block).block();

        unsafe {
            for i in full_signal.as_mut_slice() {
                *i = rng.gen_range(0.0..1.0);
            }
            let full_signal_clone = full_signal.clone();
            full_signal_array
                .dist_iter_mut()
                .enumerate()
                .for_each(move |(i, x)| *x = full_signal_clone.as_mut_slice()[i])
                .block();
            full_signal_array.barrier();

            partial_spectrum.put_buffer_unmanaged(my_pe, 0, full_spectrum.sub_region(0..array_len));
            partial_sum.put_buffer_unmanaged(my_pe, 0, magic.clone());
            partial_signal.put_buffer_unmanaged(
                my_pe,
                0,
                full_signal.sub_region(my_pe * array_len..my_pe * array_len + array_len),
            );

            for i in magic.as_mut_slice() {
                *i = MAGIC;
            }
        }
        world.wait_all();
        println!("finished init");
        world.barrier();
        println!("starting");

        let mut ti = 0;

        let mut times = vec![vec![]; 7];
        for _i in 0..num_trials {
            ti = 0;
            // if my_pe == 0 {
            // println!("trial {:?}",i);
            // }
            //--------------lamellar Manual Active Message--------------------------
            times[ti].push(dft_lamellar(
                &world,
                my_pe,
                num_pes,
                partial_signal.clone(),
                global_len,
                partial_spectrum.clone(),
            ));
            if my_pe == 0 {
                println!("am i: {:?} {:?} sum: {:?}", _i, times[ti].last(), unsafe {
                    partial_spectrum.as_slice().iter().sum::<f64>()
                });
            }
            //-----------------------------------------------------
            unsafe {
                partial_spectrum
                    .as_mut_slice()
                    .iter_mut()
                    .for_each(|e| *e = 0.0);
            }
            world.barrier();
            ti += 1;

            //--------------lamellar Manual Active Message tg--------------------------
            times[ti].push(dft_lamellar_am_group(
                &world,
                my_pe,
                num_pes,
                partial_signal.clone(),
                global_len,
                partial_spectrum.clone(),
            ));
            if my_pe == 0 {
                println!(
                    "am group i: {:?} {:?} sum: {:?}",
                    _i,
                    times[ti].last(),
                    unsafe { partial_spectrum.as_slice().iter().sum::<f64>() }
                );
            }
            unsafe {
                partial_spectrum
                    .as_mut_slice()
                    .iter_mut()
                    .for_each(|e| *e = 0.0);
            }
            //-----------------------------------------------------

            world.barrier();
            ti += 1;

            //--------------lamellar Manual Active Message group with static--------------------------
            times[ti].push(dft_lamellar_am_group_static(
                &world,
                my_pe,
                num_pes,
                partial_signal.clone(),
                global_len,
                partial_spectrum.clone(),
            ));
            if my_pe == 0 {
                println!(
                    "am group static i: {:?} {:?} sum: {:?}",
                    _i,
                    times[ti].last(),
                    unsafe { partial_spectrum.as_slice().iter().sum::<f64>() }
                );
            }
            unsafe {
                partial_spectrum
                    .as_mut_slice()
                    .iter_mut()
                    .for_each(|e| *e = 0.0);
            }
            //-----------------------------------------------------

            world.barrier();
            ti += 1;

            //--------------lamellar array--------------------------
            unsafe {
                full_spectrum_array
                    .dist_iter_mut()
                    .for_each(|elem| *elem = 0.0)
                    .block();
            }
            full_spectrum_array.wait_all();
            full_spectrum_array.barrier();

            // let timer = Instant::now();
            // times[1].push(dft_lamellar_array(
            //     full_signal_array.clone(),
            //     full_spectrum_array.clone(),
            // ));
            // let time = timer.elapsed().as_secs_f64();
            // if my_pe == 0 {
            //     println!(
            //         "{:?} array sum: {:?} time: {:?}",
            //         my_pe,
            //         full_spectrum_array.sum().block(),
            //         time
            //     );
            // }
            //-----------------------------------------------------

            world.barrier();
            // //--------------lamellar array swapped--------------------------
            // full_spectrum_array
            //     .dist_iter_mut()
            //     .for_each(|elem| *elem = 0.0);
            // full_spectrum_array.wait_all();
            // full_spectrum_array.barrier();

            // let timer = Instant::now();
            // dft_lamellar_array_swapped(full_signal_array.clone(), full_spectrum_array.clone());
            // let time = timer.elapsed().as_secs_f64();
            // if my_pe == 0 {
            //     println!(
            //         "{:?} array sum: {:?} time: {:?}",
            //         my_pe,
            //         full_spectrum_array.sum().block(),
            //         time
            //     );
            // }
            // //-----------------------------------------------------

            // world.barrier();

            //------------optimized lamellar array----------------
            unsafe {
                full_spectrum_array
                    .dist_iter_mut()
                    .for_each(|elem| *elem = 0.0)
                    .block();
            }
            full_spectrum_array.wait_all();
            full_spectrum_array.barrier();
            // let timer = Instant::now();
            times[ti].push(dft_lamellar_array_opt(
                full_signal_array.clone(),
                full_spectrum_array.clone(),
                buf_amt,
            ));
            if my_pe == 0 {
                println!("ua i: {:?} {:?}", _i, times[ti].last());
            }
            ti += 1;

            //--------------lamellar array--------------------------
            unsafe {
                full_spectrum_array
                    .dist_iter_mut()
                    .for_each(|elem| *elem = 0.0)
                    .block();
            }
            full_spectrum_array.wait_all();
            full_spectrum_array.barrier();
            times[ti].push(dft_lamellar_array_opt_test(
                full_signal_array.clone(),
                full_spectrum_array.clone(),
                buf_amt,
            ));
            if my_pe == 0 {
                println!("uat i: {:?} {:?}", _i, times[ti].last());
            }
            // let time = timer.elapsed().as_secs_f64();
            // if my_pe == 0 {
            //     println!(
            //         "{:?} array sum: {:?} time: {:?}",
            //         my_pe,
            //         full_spectrum_array.sum().block(),
            //         time
            //     );
            // }
            // if my_pe == 0 {
            // println!("---------------------");
            // }
            ti += 1;
        }
        world.barrier();

        let _ti_temp = ti;

        // full_spectrum_array
        //     .dist_iter_mut()
        //     .for_each(|elem| *elem = 0.0);
        // full_spectrum_array.wait_all();
        // full_spectrum_array.barrier();
        let full_signal_array = full_signal_array.into_read_only().block();
        let full_spectrum_array = full_spectrum_array.into_atomic().block();

        for _i in 0..num_trials {
            // let timer = Instant::now();
            // times[3].push(dft_lamellar_array_2(
            //     full_signal_array.clone(),
            //     full_spectrum_array.clone(),
            // ));

            world.barrier();
            full_spectrum_array
                .dist_iter_mut()
                .for_each(|elem| elem.store(0.0))
                .block();
            full_spectrum_array.wait_all();
            full_spectrum_array.barrier();
            // let timer = Instant::now();
            times[ti].push(dft_lamellar_array_opt_2(
                full_signal_array.clone(),
                full_spectrum_array.clone(),
                buf_amt,
            ));
            if my_pe == 0 {
                println!("aa i: {:?} {:?}", _i, times[ti].last());
            }
        }
        ti += 1;

        let full_spectrum_array = full_spectrum_array.into_local_lock().block();
        for _i in 0..num_trials {
            // let timer = Instant::now();
            times[ti].push(dft_lamellar_array_opt_3(
                full_signal_array.clone(),
                full_spectrum_array.clone(),
                buf_amt,
            ));

            world.barrier();
            full_spectrum_array
                .dist_iter_mut()
                .for_each(|elem| *elem = 0.0)
                .block();
            full_spectrum_array.wait_all();
            full_spectrum_array.barrier();
            if my_pe == 0 {
                println!("lla i: {:?} {:?}", _i, times[ti].last());
            }
        }
        // ti += 1;

        if my_pe == 0 {
            println!(
                "am time: {:?}",
                times[0].iter().sum::<f64>() / times[0].len() as f64
            );
            println!(
                "am group time: {:?}",
                times[1].iter().sum::<f64>() / times[1].len() as f64
            );
            println!(
                "am group static time: {:?}",
                times[2].iter().sum::<f64>() / times[2].len() as f64
            );
            println!(
                "optimized UnsafeArray time: {:?}",
                times[3].iter().sum::<f64>() / times[3].len() as f64
            );
            println!(
                "optimized UnsafeArray test time: {:?}",
                times[4].iter().sum::<f64>() / times[4].len() as f64
            );
            println!(
                "optimized AtomicArray time: {:?}",
                times[5].iter().sum::<f64>() / times[5].len() as f64
            );
            println!(
                "optimized LocalLockArray time: {:?}",
                times[6].iter().sum::<f64>() / times[6].len() as f64
            );
        }
    } else {
        //-----------------------------------------------------
        let mut full_signal = vec![0.0; array_len];
        let mut full_spectrum = vec![0.0; array_len];
        for i in full_signal.iter_mut() {
            *i = rng.gen_range(0.0..1.0);
        }

        let mut times = vec![vec![]; 2];
        for i in 0..10 {
            if i == 0 {
                // let timer = Instant::now();
                times[0].push(dft_serial(&full_signal, &mut full_spectrum));
            }
            // let time = timer.elapsed().as_secs_f64();
            // println!(
            //     "serial sum: {:?} time: {:?}",
            //     full_spectrum.iter().sum::<f64>(),
            //     time
            // );
            // let timer = Instant::now();
            times[1].push(dft_rayon(&full_signal, &mut full_spectrum));
            // let time = timer.elapsed().as_secs_f64();
            // println!(
            //     "rayon sum: {:?} time: {:?}",
            //     full_spectrum.iter().sum::<f64>(),
            //     time
            // );
        }
        println!(
            "serial time: {:?}",
            times[0].iter().sum::<f64>() / times[0].len() as f64
        );
        println!(
            "rayon time: {:?}",
            times[1].iter().sum::<f64>() / times[1].len() as f64
        );
    }
    //-----------------------------------------------------
}
