/// ----------------Lamellar Parallel Array GEMM---------------------------------------------------
/// This performs a distributed GEMM, iteratively performing dot products of rows from the A matrix
/// with columns fro the B matrix. Each pe only iterates over the local rows of the A matrix simultaneously,
/// and thus consequently must iterate over all the columns from the B matrix. Our loops are ordered
/// such that we get a column of B and perform the dot product for each (local) row of A. This ensures
/// we only transfer each column of B once (to each PE). This formulation also allows only requiring
/// local updates to the C matrix.
///----------------------------------------------------------------------------------
use lamellar::array::prelude::*;


fn main() {
    let args: Vec<String> = std::env::args().collect();
    let elem_per_pe = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 200);

    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let dim = elem_per_pe * num_pes;

    //for example purposes we are multiplying square matrices
    let m = dim; //a & c rows
    let n = dim; // a cols b rows
    let p = dim; // b & c cols

    println!("m: {}, n: {}, p: {}", m, n, p);

    let a = LocalLockArray::<f32>::new(&world, m * n, Distribution::Block); //row major
    let b = LocalLockArray::<f32>::new(&world, n * p, Distribution::Block); //col major
    let c = AtomicArray::<f32>::new(&world, m * p, Distribution::Block); //row major

    //initialize matrices
    a.dist_iter_mut()
        .enumerate()
        .for_each(|(i, x)| *x = i as f32);
    b.dist_iter_mut().enumerate().for_each(move |(i, x)| {
        //need global index so use dist_iter
        //identity matrix
        let row = i / dim;
        let col = i % dim;
        if row == col {
            *x = 1 as f32
        } else {
            *x = 0 as f32;
        }
    });
    c.dist_iter_mut().for_each(|x| x.store(0.0));
    

    world.wait_all();
    world.barrier();
    let a = a.into_read_only();
    let b = b.into_read_only();

    let num_gops = ((2 * dim * dim * dim) - dim * dim) as f64 / 1_000_000_000.0; // accurate for square matrices

    // transfers columns of b to each pe,
    // parallelizes over rows of a
    let rows_pe = m / num_pes;
    let start = std::time::Instant::now();
    b.onesided_iter() // OneSidedIterator (each pe will iterate through entirety of b)
        .chunks(p) //chunk flat array into columns -- manages efficent transfer and placement of data into a local memory region
        .into_iter() // convert into normal rust iterator
        .enumerate()
        .for_each(|(j, col)| {
            let col = col.clone();
            let c = c.clone();
            a.local_iter() //LocalIterator (each pe will iterate through only its local data -- in parallel)
                .chunks(n) // chunk by the row size
                .enumerate()
                .for_each(move |(i, row)| {
                    let sum = unsafe{col.iter().zip(row).map(|(&i1, &i2)| i1 * i2).sum::<f32>()}; // dot product using rust iters... but MatrixMultiply is faster
                    // let _lock = LOCK.lock(); //this lock is to protect writes into c, is c was an AtomicArray or LocalLockArray we could remove this
                    c.mut_local_data().at(j + (i % rows_pe) * m).fetch_add(sum);
                    //we know all updates to c are local so directly update the raw data
                    //we could also use:
                    //c.add(j+i*m,sum) -- but some overheads are introduce from PGAS calculations performed by the runtime, and since its all local updates we can avoid them
                });
        });
    
    world.wait_all();
    world.barrier();
    let elapsed = start.elapsed().as_secs_f64();

    println!("Elapsed: {:?}", elapsed);
    if my_pe == 0 {
        println!("elapsed {:?} Gflops: {:?}", elapsed, num_gops / elapsed,);
    }
    // unsafe {
    //     world.block_on(c.dist_iter_mut().enumerate().for_each(|(i, x)| {
    //         //check that c == a
    //         if *x != i as f32 {
    //             println!("error {:?} {:?}", x, i);
    //         }
    //         *x = 0.0
    //     }));
    // }
}
