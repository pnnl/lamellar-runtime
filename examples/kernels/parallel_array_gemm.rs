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
use lamellar::ActiveMessaging;
use lamellar::array::{SerialIterator, DistributedIterator, Distribution, UnsafeArray};
use lazy_static::lazy_static;
use parking_lot::Mutex;
lazy_static! {
    static ref LOCK: Mutex<()> = Mutex::new(());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let elem_per_pe = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 2000);

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let dim = elem_per_pe * num_pes;

    //for example purposes we are multiplying square matrices
    let m = dim; //a & c rows
    let n = dim; // a cols b rows
    let p = dim; // b & c cols

    let a = UnsafeArray::<f32>::new(&world, m * n, Distribution::Block);//row major
    let b = UnsafeArray::<f32>::new(&world, n * p, Distribution::Block);//col major
    let c = UnsafeArray::<f32>::new(&world, m * p, Distribution::Block);//row major
    a.dist_iter_mut().enumerate().for_each(|(i,x)| *x = i as f32 );
    b.dist_iter_mut().enumerate().for_each(move|(i,x)| { //identity matrix
        let row = i/dim;
        let col = i%dim;
        if row==col{
            *x = 1 as f32
        }
        else {
            *x =0 as f32;
        } 
    });
    c.dist_iter_mut().for_each(|x| *x = 0.0 );

    // a.print();
    // b.print();
    world.wait_all();
    world.barrier();

    let num_gops = ((2 * dim * dim * dim) - dim * dim) as f64 / 1_000_000_000.0; // accurate for square matrices

    
    // transfers columns of b to each pe,
    // parallelizes over rows of a
    let rows_pe = m/num_pes;
    let start = std::time::Instant::now();
    b.ser_iter().copied_chunks(n).into_iter().enumerate().for_each(|(j,col)| {
        let col = col.clone();
        let c = c.clone();
        a.dist_iter().chunks(m).enumerate().for_each(move |(i,row)| {
            // println!("{:?} ",col.as_slice().unwrap());
            let sum = col.iter().zip(row).map(|(&i1,&i2)| i1*i2).sum::<f32>();
            // println!("sum {:?}",sum);
            let _lock = LOCK.lock();
            c.local_as_mut_slice()[j+(i%rows_pe)*m] += sum;
        });
    });
    world.wait_all();
    world.barrier();
    let elapsed = start.elapsed().as_secs_f64();
    // c.print();
    println!("Elapsed: {:?}",elapsed);
    if my_pe == 0 {
        println!(
            "elapsed {:?} Gflops: {:?}",
            elapsed,
            num_gops / elapsed,
        );
    }
    c.dist_iter_mut().enumerate().for_each(|(i,x)| {
        if *x != i as f32 {
            println!("error {:?} {:?}",x,i);
        }
        *x = 0.0 
    })
    
    
}