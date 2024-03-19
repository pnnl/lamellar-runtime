use futures_util::stream::StreamExt;
use lamellar::array::prelude::*;
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

#[lamellar::AmData(Dist, Debug, ArrayOps, Copy, Clone, Default)]
struct Block {
    j: usize,
    k: usize,
}

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

    let a = LocalLockArray::<f32>::new(&world, m * n, Distribution::Block); //row major
    let b = LocalLockArray::<f32>::new(&world, n * p, Distribution::Block); //col major
    let c = AtomicArray::<f32>::new(&world, m * p, Distribution::Block); //row major
                                                                         //initialize
    let _ = a
        .dist_iter_mut()
        .enumerate()
        .for_each(|(i, x)| *x = i as f32);
    let _ = b.dist_iter_mut().enumerate().for_each(move |(i, x)| {
        //identity matrix
        let row = i / dim;
        let col = i % dim;
        if row == col {
            *x = 1 as f32
        } else {
            *x = 0 as f32;
        }
    });
    let _ = c.dist_iter_mut().for_each(|x| x.store(0.0));
    world.wait_all();
    world.barrier();
    let a = a.into_read_only();
    let b = b.into_read_only();

    let num_gops = ((2 * dim * dim * dim) - dim * dim) as f64 / 1_000_000_000.0; // accurate for square matrices
    let blocksize = dim / num_pes;
    let m_blks = m / blocksize; //a blk row  c blk row
    let m_blks_pe = m_blks / num_pes;
    let n_blks = n / blocksize; //a blk col b blk col(because b is col major)
    let p_blks = p / blocksize; // b blk row(b is col major) c blk col

    println! {"n: {}, m: {}, p: {}, blocksize: {}, m_blks: {}, n_blks: {}, p_blks: {}, m_blks_pe: {}, num_gops: {}", n, m, p, blocksize, m_blks, n_blks, p_blks, m_blks_pe, num_gops};

    // this is a "hack" until we support something like (0..n_blks).dist_iter()
    // we construct a global array where each pe will contain the sequence (0..n_blks)
    // we can then call dist_iter() on this array to iterate over the range in parallel on each PE
    let nblks_array =
        LocalLockArray::<Block>::new(&world, (n_blks * n_blks) * num_pes, Distribution::Block);
    world.block_on(
        nblks_array
            .dist_iter_mut()
            .enumerate()
            .for_each(move |(g_i, x)| {
                let i = g_i % (n_blks * n_blks);
                x.j = i / n_blks;
                x.k = i % n_blks
            }),
    );
    let nblks_array = nblks_array.into_read_only();

    let start = std::time::Instant::now();
    let a_clone = a.clone();
    let b_clone = b.clone();
    let c_clone = c.clone();
    let _ = nblks_array.dist_iter().for_each_async(move |block| {
        let b = b_clone.clone();
        let a: ReadOnlyArray<f32> = a_clone.clone();
        let c = c_clone.clone();
        async move {
            //iterate over the submatrix cols of b, use dist_iter() so that we can launch transfers in parallel
            // iterate over submatrix rows of b
            let j_blk = block.j;
            let k_blk = block.k;
            // println!("j_blk: {}, k_blk: {}", j_blk, k_blk);
            let b_block = b
                .onesided_iter() // OneSidedIterator (each pe will iterate through entirety of b)
                .chunks(blocksize) //chunks columns by blocksize  -- manages efficent transfer and placement of data into a local memory region
                .skip(k_blk * n_blks * blocksize + j_blk) // skip previously transfered submatrices
                .step_by(n_blks) //grab chunk from next column in submatrix
                // .buffered(100)
                .into_stream() // convert to normal rust iterator
                .take(blocksize) // we only need to take blocksize columns
                .collect::<Vec<_>>()
                .await; //gather local memory regions containing each columns data
                        // println!("here");
                        //need to store the submatrix in a contiguous memory segment for use with the MatrixMultiply library
            let mut b_block_vec = vec![0.0; blocksize * blocksize];
            for (j, col) in b_block.iter().enumerate() {
                //(index, LocalMemRegion)
                let b_block_col = &mut b_block_vec[j * blocksize..(j + 1) * blocksize];
                b_block_col.copy_from_slice(unsafe { col.as_slice().unwrap() });
            }
            let b_block_vec = Arc::new(b_block_vec); //we will be sharing this submatrix in multiple tasks
                                                     //--------------

            for i_blk in 0..m_blks_pe {
                // iterate of the local submatrix rows of a
                let b_block_vec = b_block_vec.clone();
                let a_vec = a
                    .local_as_slice()
                    .chunks(blocksize)
                    .skip(i_blk * m_blks * blocksize + k_blk) //skip previously visited submatrices
                    .step_by(m_blks) //grab chunk from the next row in submatrix
                    .take(blocksize) //we only need to take blocksize rows
                    .flatten()
                    .copied() //get values instead of references
                    .collect::<Vec<f32>>();
                // -------------------------------
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

                let c_slice = c.mut_local_data();

                for row in 0..blocksize {
                    let row_offset = (i_blk * blocksize + row) * n;
                    for col in 0..blocksize {
                        let col_offset = j_blk * blocksize + col;
                        c_slice
                            .at(row_offset + col_offset)
                            .fetch_add(c_vec[row * blocksize + col]);
                        //we know all updates to c are local so directly update the raw data
                        // we could use the array.add interface by calculating the global index: let g_i_blk = i_blk + my_pe *m_blks_pe; and replacing it in row_offset
                        // c.add(row_offset+col_offset,c_vec[row*blocksize + col]); -- but some overheads are introduce from PGAS calculations performed by the runtime, and since its all local updates we can avoid them
                    }
                }
            }
        }
        // }
    });
    world.wait_all();
    world.barrier();
    let elapsed = start.elapsed().as_secs_f64();

    if my_pe == 0 {
        println!("Elapsed: {:?}", elapsed);
        println!(
            "blksize: {:?} elapsed {:?} Gflops: {:?} MBsend: {:?}",
            blocksize,
            elapsed,
            num_gops / elapsed,
            world.MB_sent()
        );
    }
}
