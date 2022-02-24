use lamellar::array::{
    DistributedIterator, Distribution, LocalLockAtomicArray, SerialIterator, UnsafeArray,
};
/// ----------------Lamellar Parallel Blocked Array GEMM---------------------------------------------------
/// This performs a distributed GEMM by partitioning the global matrices (stored in LamellarArrya)
/// into sub matrices, and then performing GEMMS on the sub matrices.
/// Each PE iterates of the local submatrices of the A matrix simultaneously, and thus each PE
/// iterates over all sub matrices from the B matrix. To ensurse each submatrix of B is only
/// transfered to each PE once, we first iterate over B submatrices in the outer loop
/// and then iterate over the local A submatrices in the inner loop. Finally, all updates
/// to the C matrix are only performed locally, requiring no additional data transfer.
///----------------------------------------------------------------------------------
use matrixmultiply::sgemm;
use std::sync::Arc;

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

    let a = UnsafeArray::<f32>::new(&world, m * n, Distribution::Block); //row major -- we will change this into a readonly array after initialization
    let b = UnsafeArray::<f32>::new(&world, n * p, Distribution::Block); //col major -- we will change this into a readonly array after initialization
    let c = LocalLockAtomicArray::<f32>::new(&world, m * p, Distribution::Block); //row major
                                                                                  //initialize
    a.dist_iter_mut()
        .enumerate()
        .for_each(|(i, x)| *x = i as f32);
    b.dist_iter_mut().enumerate().for_each(move |(i, x)| {
        //identity matrix
        let row = i / dim;
        let col = i % dim;
        if row == col {
            *x = 1 as f32
        } else {
            *x = 0 as f32;
        }
    });
    c.dist_iter_mut().for_each(|x| *x = 0.0);
    let a = a.into_read_only();
    let b = b.into_read_only();
    c.wait_all();
    c.barrier();

    let num_gops = ((2 * dim * dim * dim) - dim * dim) as f64 / 1_000_000_000.0; // accurate for square matrices
    let blocksize = std::cmp::min(2000, dim / num_pes);
    let m_blks = m / blocksize; //a blk row  c blk row
    let m_blks_pe = m_blks / num_pes;
    let n_blks = n / blocksize; //a blk col b blk col(because b is col major)
    let p_blks = p / blocksize; // b blk row(b is col major) c blk col

    // this is a "hack" until we support something like (0..n_blks).dist_iter()
    // we construct a global array where each pe will contain the sequence (0..n_blks)
    // we can then call dist_iter() on this array to iterate over the range in parallel on each PE
    let nblks_array = UnsafeArray::new(&world, n_blks * num_pes, Distribution::Block);
    nblks_array
        .dist_iter_mut()
        .enumerate()
        .for_each(move |(i, x)| *x = i % n_blks);

    let start = std::time::Instant::now();
    let a = a.clone();
    let b = b.clone();
    let c_clone = c.clone();
    nblks_array.dist_iter().for_each(move |k_blk| {
        //iterate over the submatrix cols of b, use dist_iter() so that we can launch transfers in parallel
        for j_blk in 0..p_blks {
            // iterate over submatrix rows of b

            let b_block = b
                .ser_iter() // SerialIterator (each pe will iterate through entirety of b)
                .copied_chunks(blocksize) //chunks columns by blocksize  -- manages efficent transfer and placement of data into a local memory region
                .ignore(*k_blk * n_blks * blocksize + j_blk) // skip previously transfered submatrices
                .step_by(n_blks) //grab chunk from next column in submatrix
                .into_iter() // convert to normal rust iterator
                .take(blocksize) // we only need to take blocksize columns
                .collect::<Vec<_>>(); //gather local memory regions containing each columns data

            //need to store the submatrix in a contiguous memory segment for use with the MatrixMultiply library
            let mut b_block_vec = vec![0.0; blocksize * blocksize];
            for (j, col) in b_block.iter().enumerate() {
                for (i, elem) in col.as_slice().unwrap().iter().enumerate() {
                    b_block_vec[j * blocksize + i] = *elem
                }
            }
            let b_block_vec = Arc::new(b_block_vec); //we will be sharing this submatrix in multiple tasks
                                                     //--------------

            for i_blk in 0..m_blks_pe {
                // iterate of the local submatrix rows of a
                let c = c_clone.clone();
                let b_block_vec = b_block_vec.clone();
                a.dist_iter() //DistributedIterator (each pe will iterate through only its local data -- in parallel)
                    .chunks(blocksize) //chunks rows by blocksize
                    .ignore(i_blk * m_blks * blocksize + *k_blk) //ignore previously visited submatrices
                    .step_by(m_blks) //grab chunk from the next row in submatrix
                    .take(blocksize) //we only need to take blocksize rows
                    .chunks(blocksize) //currently a "hack" for Iterate::collect()
                    .for_each(move |a_block| {
                        //iterate over local submatrices is submatrix row "i_blk"
                        //need to store the submatrix in a contiguous memory segment for use with the MatrixMultiply library
                        let mut a_vec = vec![0.0; blocksize * blocksize];
                        for (i, row) in a_block.enumerate() {
                            for (j, elem) in row.enumerate() {
                                a_vec[i * blocksize + j] = *elem;
                            }
                        }
                        //-------------------------------
                        let mut c_vec = vec![0.0; blocksize * blocksize]; // MatrixMultiple lib stores result in a contiguous memory segment
                        unsafe {
                            sgemm(
                                blocksize,
                                blocksize,
                                blocksize,
                                1.0,
                                a_vec.as_ptr(),
                                blocksize as isize,
                                1,
                                b_block_vec.as_ptr(),
                                1,
                                blocksize as isize,
                                0.0,
                                c_vec.as_mut_ptr(),
                                blocksize as isize,
                                1,
                            );
                        }

                        let mut c_slice = c.local_as_mut_slice(); //this locks the array

                        for row in 0..blocksize {
                            let row_offset = (i_blk * blocksize + row) * n;
                            for col in 0..blocksize {
                                let col_offset = j_blk * blocksize + col;
                                c_slice[row_offset + col_offset] += c_vec[row * blocksize + col];
                                //we know all updates to c are local so directly update the raw data
                                // we could use the array.add interface by calculating the global index: let g_i_blk = i_blk + my_pe *m_blks_pe; and replacing it in row_offset
                                // c.add(row_offset+col_offset,c_vec[row*blocksize + col]); -- but some overheads are introduce from PGAS calculations performed by the runtime, and since its all local updates we can avoid them
                            }
                        }
                    });
            }
        }
    });
    world.wait_all();
    world.barrier();
    let elapsed = start.elapsed().as_secs_f64();

    if my_pe == 0 {
        println!("Elapsed: {:?}", elapsed);
        println!(
            "blksize: {:?} elapsed {:?} Gflops: {:?}",
            blocksize,
            elapsed,
            num_gops / elapsed,
        );
    }
    // c.dist_iter_mut().enumerate().for_each(|(i, x)| {
    //     if *x != i as f32 {
    //         println!("error {:?} {:?}", x, i);
    //     }
    //     *x = 0.0
    // });
}
