
/// ------------Lamellar Bandwidth: DFT Proxy  -------------------------
/// This example is inspired from peforming a naive DFT
/// it does not actually calculate a DFT as we simply perform the transform
/// using f64s, rather than using complex representations.
/// we include the distributed Lamellar Implemtation
/// as well as a (single process) shared memory version using Rayon.
/// --------------------------------------------------------------------
use lamellar::{ActiveMessaging, LamellarWorld};
use lamellar::array::{Distribution, DistributedIterator, UnsafeArray};
use lamellar::{RemoteMemoryRegion, SharedMemoryRegion};
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
    fn exec(self) -> f64 {
        self.spectrum.as_slice().unwrap().iter().sum::<f64>()
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

#[lamellar::am]
impl LamellarAM for LocalSumAM {
    fn exec() {
        let spectrum_slice = unsafe { self.spectrum.as_mut_slice().unwrap() };
        let k_prime = self.k + self.pe * spectrum_slice.len();
        let signal = self.signal.as_slice().unwrap();
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
struct RemoteSumAM {
    spectrum: SharedMemoryRegion<f64>,
    add_spec: Vec<f64>,
}

#[lamellar::am]
impl LamellarAM for RemoteSumAM {
    fn exec(self) {
        let _lock = LOCK.lock();
        for (k, spec_bin) in unsafe { self.spectrum.as_mut_slice().unwrap().iter_mut().enumerate() }
        {
            *spec_bin += self.add_spec[k];
        }
    }
}

fn dft_lamellar(
    world: &LamellarWorld,
    my_pe: usize,
    num_pes: usize,
    signal: SharedMemoryRegion<f64>,
    global_sig_len: usize,
    spectrum: SharedMemoryRegion<f64>,
) {
    let spectrum_slice = spectrum.as_slice().unwrap();
    let add_spec = world.alloc_shared_mem_region::<f64>(spectrum_slice.len());

    let timer = Instant::now();
    for pe in 0..num_pes {
        for k in 0..spectrum_slice.len() {
            world.exec_am_pe(
                my_pe,
                LocalSumAM {
                    spectrum: add_spec.clone(),
                    signal: signal.clone(),
                    global_sig_len: global_sig_len,
                    k: k,
                    pe: pe,
                },
            );
        }
        let mut add_spec_vec = vec![0.0; spectrum_slice.len()];
        world.wait_all();
        add_spec_vec.copy_from_slice(add_spec.as_slice().unwrap());
        world.exec_am_pe(
            pe,
            RemoteSumAM {
                spectrum: spectrum.clone(),
                add_spec: add_spec_vec,
            },
        );
    }
    world.wait_all();
    world.barrier();
    if my_pe == 0 {
        let res = world
            .exec_am_all(ReduceAM {
                spectrum: spectrum.clone(),
            })
            .get_all();
        let sum = res.iter().map(|x| x.unwrap_or(0.0)).sum::<f64>();
        let time = timer.elapsed().as_secs_f64();
        println!("distributed sum: {:?} {:?}", sum, time);
    }
    world.barrier();
}

// fn dft_serial(signal: &[f64], spectrum: &mut [f64]) {
//     for (k, spec_bin) in spectrum.iter_mut().enumerate() {
//       let mut sum = 0f64;
//       for (i, &x) in signal.iter().enumerate() {
//         let angle = -1f64 * (i * k) as f64 * 2f64 * std::f64::consts::PI / signal.len() as f64;
//         let twiddle = angle * (angle.cos() + angle*angle.sin());
//         sum = sum + twiddle * x;
//       }
//       *spec_bin = sum;
//     }
// }

fn dft_rayon(signal: &[f64], spectrum: &mut [f64]) {
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
        })
}

// the easiest implementation using lamellar arrays, although not terribly performance
// because each iteration of the outer loop is transferring the entirety of the signal array
// without an reuse, using a buffered_iter helps to ensure the transfers are efficient, but
// a lot of (needless) data transfer happens
fn dft_lamellar_array(signal: UnsafeArray<f64>, spectrum: UnsafeArray<f64>) {
    let signal_clone = signal.clone();
    spectrum
        .dist_iter_mut()
        .enumerate()
        .for_each(move |(k, spec_bin)| {
            let mut sum = 0f64;
            for (i, &x) in signal_clone.buffered_iter(1000).enumerate() {
                let angle = -1f64 * (i * k) as f64 * 2f64 * std::f64::consts::PI
                    / signal_clone.len() as f64;
                let twiddle = angle * (angle.cos() + angle * angle.sin());
                sum = sum + twiddle * x;
            }
            *spec_bin = sum
        });
    spectrum.wait_all();
    spectrum.barrier();
}


// a more optimized version using lamellar arrays, and behaves similar to the manual active message implementation above
// here we create "copied chunks" of the signal array, which are then passed and reused during each iteration of the spectrum loop.
// in this case we only completely transfer the signal array one time
fn dft_lamellar_array_opt(signal: UnsafeArray<f64>, spectrum: UnsafeArray<f64>, buf_size: usize) {
    let sig_len = signal.len();
    signal
        .iter()
        .copied_chunks(buf_size)
        .enumerate()
        .for_each(|(i, chunk)| {
            let signal = chunk.clone();
            spectrum
                .dist_iter_mut()
                .enumerate()
                .for_each(move |(k, spec_bin)| {
                    let mut sum = 0f64;
                    for (j, &x) in signal
                        .iter()
                        .enumerate()
                        .map(|(j, x)| (j + i * buf_size, x))
                    {
                        let angle =
                            -1f64 * (j * k) as f64 * 2f64 * std::f64::consts::PI / sig_len as f64;
                        let twiddle = angle * (angle.cos() + angle * angle.sin());
                        sum = sum + twiddle * x;
                    }
                    let _lock = LOCK.lock();
                    *spec_bin += sum;
                });
        });
    spectrum.wait_all();
    spectrum.barrier();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let array_len = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 1000);

    let run_single_node = args
        .get(2)
        .and_then(|s| {
            if s == "--run-local" {
                Some(true)
            } else {
                Some(false)
            }
        })
        .unwrap_or_else(|| false);
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let global_len = num_pes * array_len;
    let mut rng = StdRng::seed_from_u64(10);
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);
    let partial_sum = world.alloc_shared_mem_region::<f64>(num_pes);
    let partial_spectrum = world.alloc_shared_mem_region::<f64>(array_len);
    let partial_signal = world.alloc_shared_mem_region::<f64>(array_len);
    let full_signal = world.alloc_local_mem_region::<f64>(global_len);
    let full_spectrum = world.alloc_local_mem_region::<f64>(global_len);
    let magic = world.alloc_local_mem_region::<f64>(num_pes);

    let full_spectrum_array = UnsafeArray::<f64>::new(world.team(), global_len, Distribution::Block);
    let full_signal_array = UnsafeArray::<f64>::new(world.team(), global_len, Distribution::Block);

    unsafe {
        for i in full_signal.as_mut_slice().unwrap() {
            *i = rng.gen_range(0.0, 1.0);
        }
        let full_signal_clone = full_signal.clone();
        full_signal_array
            .dist_iter_mut()
            .enumerate()
            .for_each(move |(i, x)| *x = full_signal_clone.as_mut_slice().unwrap()[i]);
        full_signal_array.wait_all();
        full_signal_array.barrier();
   
        
        partial_spectrum.put(my_pe, 0, full_spectrum.sub_region(0..array_len));
        partial_sum.put(my_pe, 0, magic.clone());
        partial_signal.put(
            my_pe,
            0,
            full_signal.sub_region(my_pe * array_len..my_pe * array_len + array_len),
        );

        for i in magic.as_mut_slice().unwrap() {
            *i = MAGIC;
        }
    }

    println!("finished init");
    world.barrier();
    println!("starting");


    //--------------lamellar Manual Active Message--------------------------
    dft_lamellar(
        &world,
        my_pe,
        num_pes,
        partial_signal.clone(),
        global_len,
        partial_spectrum.clone(),
    );
    //-----------------------------------------------------

    world.barrier();

    //--------------lamellar array--------------------------
    full_spectrum_array
        .dist_iter_mut()
        .for_each(|elem| *elem = 0.0);
    full_spectrum_array.wait_all();
    full_spectrum_array.barrier();

    let timer = Instant::now();
    dft_lamellar_array(full_signal_array.clone(), full_spectrum_array.clone());
    let time = timer.elapsed().as_secs_f64();
    println!(
        "{:?} array sum: {:?} time: {:?}",
        my_pe,
        full_spectrum_array.sum().get(),
        time
    );
    //-----------------------------------------------------

    world.barrier();


    //------------optimized lamellar array----------------
    full_spectrum_array
        .dist_iter_mut()
        .for_each(|elem| *elem = 0.0);
    full_spectrum_array.wait_all();
    full_spectrum_array.barrier();
    let timer = Instant::now();
    dft_lamellar_array_opt(full_signal_array.clone(), full_spectrum_array.clone(), 100);
    let time = timer.elapsed().as_secs_f64();
    println!(
        "{:?} array sum: {:?} time: {:?}",
        my_pe,
        full_spectrum_array.sum().get(),
        time
    );
    //-----------------------------------------------------

   
    //--------------------rayon---------------------
    if run_single_node {
        let timer = Instant::now();
        dft_rayon(full_signal_array.local_as_slice(), full_spectrum_array.local_as_mut_slice());
        let time = timer.elapsed().as_secs_f64();
        println!(
            "rayon sum: {:?} time: {:?}",
            full_spectrum.as_slice().unwrap().iter().sum::<f64>(),
            time
        );
    }
    //-----------------------------------------------------
}
