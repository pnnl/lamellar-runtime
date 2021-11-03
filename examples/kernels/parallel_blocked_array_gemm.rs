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
use lamellar::array::{DistributedIterator,SerialIterator, Distribution, UnsafeArray};
use lazy_static::lazy_static;
use matrixmultiply::sgemm;
use parking_lot::Mutex;
use std::sync::Arc;
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
    let b = UnsafeArray::<f32>::new(&world, m * n, Distribution::Block);//col major
    let c = UnsafeArray::<f32>::new(&world, m * n, Distribution::Block);//row major
    a.dist_iter_mut().enumerate().for_each(|(i,x)| *x = i as f32 );
    b.dist_iter_mut().enumerate().for_each(move|(i,x)| {
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
    world.wait_all();
    world.barrier();

    let num_gops = ((2 * dim * dim * dim) - dim * dim) as f64 / 1_000_000_000.0; // accurate for square matrices
    let blocksize = dim/num_pes;
    let m_blks = m/blocksize;
    let m_blks_pe = m_blks/num_pes;
    let n_blks = n/blocksize;
    let p_blks =  p/blocksize;

    let nblks_array = UnsafeArray::new(&world,n_blks*num_pes,Distribution::Block);
    nblks_array.dist_iter_mut().enumerate().for_each(move |(i,x)| *x = i%n_blks );
    
    let start = std::time::Instant::now();
    let a = a.clone();
    let b = b.clone();
    let c_clone = c.clone();
    nblks_array.dist_iter_mut().for_each( move |k_blk| {        
        for j_blk in 0..p_blks{//b_col
            let mut b_block_vec = vec!{0.0;blocksize*blocksize};                    
            let b_block = b.ser_iter().copied_chunks(blocksize).ignore(*k_blk*n_blks*blocksize+j_blk).step_by(n_blks).into_iter().take(blocksize).collect::<Vec<_>>();
            for (i,row) in b_block.iter().enumerate(){
                for (j,elem) in row.as_slice().unwrap().iter().enumerate(){
                    b_block_vec[j*blocksize + i] = *elem
                }
            }
            let b_block_vec = Arc::new(b_block_vec);
            for i_blk in 0..m_blks_pe{//a row
                let c = c_clone.clone();
                let b_block_vec = b_block_vec.clone();
                a.dist_iter()
                    .chunks(blocksize) //create blocksize vectors (across cols)
                    .ignore(i_blk*m_blks*blocksize+*k_blk) //ignore previous row
                    .step_by(m_blks) //ignore rest of the row
                    .take(blocksize)// taked blocksize rows
                    .chunks(blocksize)                            
                    .for_each(move|a_block|{
                        let mut a_vec = vec!{0.0;blocksize*blocksize};
                        let mut c_vec = vec!{0.0;blocksize*blocksize};
                        for (i,row) in a_block.enumerate(){
                            for (j,elem) in row.enumerate(){
                                a_vec[i*blocksize+j]=*elem;
                                
                            }
                        }
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
                        let c_slice = c.local_as_mut_slice();
                        let _lock = LOCK.lock();
                        let g_i_blk = i_blk + my_pe *m_blks_pe;
                        for row in 0..blocksize{
                            
                            let row_offset = (i_blk*blocksize+row)*n;
                            for col in 0..blocksize{
                                let col_offset = j_blk*blocksize+col;
                                c_slice[row_offset+col_offset] += c_vec[row*blocksize + col];
                                // if row_offset+col_offset > 32000000 {
                                //     panic!("uhhh ohhhh!!! {:?} {:?} {:?} {:?} {:?} {:?}", i_blk, g_i_blk, j_blk, row, col, row_offset+col_offset);

                                // }
                                // c.add(row_offset+col_offset,c_vec[row*blocksize + col]);
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
        println!("Elapsed: {:?}",elapsed);
        println!(
            "blksize: {:?} elapsed {:?} Gflops: {:?}",
            blocksize,
            elapsed,
            num_gops / elapsed,
        );
    }  
    c.dist_iter_mut().enumerate().for_each(|(i,x)| {
        if *x != i as f32 {
            println!("error {:?} {:?}",x,i);
        }
        *x = 0.0 
    });
    
}
