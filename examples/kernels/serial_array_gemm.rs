/// ----------------Lamellar Gemm 1---------------------------------------------------
/// This naive GEMM implementation performs blockwise (tiled) mat mults
/// it does not perform any optimzation for reusing a given block
/// a.k.a remote blocks are transfered for every sub matrix multiplication
/// we launch active messages so that the result of a tiled mat mult
/// is stored to the local portion of the C matrix. That is, we never transfer
/// mat mult results over the network. To view the final matrix on a single node,
/// we would transfer the data after the multiplication
///
/// matrices use row-wise distribution (i.e. all elements of a row are local to a pe,
/// conversely this means elements of a column are distributed across pes)
///----------------------------------------------------------------------------------
use futures::future;
use lamellar::ActiveMessaging;
use lamellar::array::{DistributedIterator, Distribution, UnsafeArray};
// use lamellar::{LocalMemoryRegion, RemoteMemoryRegion, SharedMemoryRegion};
use lazy_static::lazy_static;
use matrixmultiply::sgemm;
use parking_lot::Mutex;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let elem_per_pe = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 2);

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let dim = elem_per_pe * num_pes;

    //for example purposes we are multiplying square matrices
    let m = dim; //a & c rows
    let n = dim; // a cols b rows
    let p = dim; // b & c cols

    let a = UnsafeArray::<f32>::new(&world, m * n, Distribution::Block);//row major
    let b = UnsafeArray::<f32>::new(&world, m * n, Distribution::Cyclic);//col major
    let c = UnsafeArray::<f32>::new(&world, m * n, Distribution::Block);//row major
    a.dist_iter_mut().enumerate().for_each(|(i,x)| *x = i as f32 );
    b.dist_iter_mut().for_each(|x| *x = 2.0 );
    c.dist_iter_mut().for_each(|x| *x = 0.0 );

    // a.print();
    // b.print();
    world.wait_all();
    world.barrier();

    let num_gops = ((2 * dim * dim * dim) - dim * dim) as f64 / 1_000_000_000.0; // accurate for square matrices

    if my_pe == 0 {
        println!("starting");
    }

    //The standard unoptimized serial matrix muliply algorithm, 
    let start = std::time::Instant::now();
    if my_pe == 0 {        
            for i in 0..m{
                for j in 0..p{
                    let mut sum = 0.0;
                    for k in 0..n{
                        sum += a.at(k + i*m) * b.at(j + k*n)
                    }
                    c.put(j + i*m,sum);
                }
            }
        }
    world.wait_all();
    world.barrier();
    let elapsed = start.elapsed().as_secs_f64();

    c.dist_iter_mut().for_each(|x| *x = 0.0 );
    c.wait_all();
    c.barrier();    
    println!("Elapsed: {:?}",elapsed);
    if my_pe == 0 {
        println!(
            "elapsed {:?} Gflops: {:?}",
            elapsed,
            num_gops / elapsed,
        );
    } 
}
