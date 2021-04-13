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
use lamellar::{ActiveMessaging, LamellarAM};
use lamellar::{
    LamellarLocalMemoryRegion, LamellarMemoryRegion, RegisteredMemoryRegion, RemoteMemoryRegion,
};
use lazy_static::lazy_static;
use matrixmultiply::sgemm;
use parking_lot::Mutex;

lazy_static! {
    static ref LOCK: Mutex<()> = Mutex::new(());
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct SubMatrix {
    name: String,
    mat: LamellarMemoryRegion<f32>, //handle to underlying data
    pe: usize,                      //originating be
    rows: usize,                    // number of rows in the distributed root matrix
    cols: usize,                    // number of cols in the distributed root matrix
    row_block: usize, // index of the local row block this submatrix corresponds to (0 based indexing)
    col_block: usize, // index of the local col block this submatrix corresponds to (0 based indexing)
    block_size: usize, //sub mat will be block_size*block_size
}

impl SubMatrix {
    fn new(
        name: String,
        mat: LamellarMemoryRegion<f32>,
        pe: usize,
        rows: usize,
        cols: usize,
        row_block: usize,
        col_block: usize,
        block_size: usize,
    ) -> SubMatrix {
        SubMatrix {
            name: name,
            mat: mat,
            pe: pe,
            rows: rows,
            cols: cols,
            row_block: row_block,
            col_block: col_block,
            block_size: block_size, //sub mat will be block_size*block_size
        }
    }
    fn add_mat(&self, mat: &Vec<f32>) {
        let start_row = self.row_block * self.block_size;
        let start_col = self.col_block * self.block_size;
        let raw = unsafe { self.mat.as_mut_slice().unwrap() };
        let _lock = LOCK.lock(); //we are operating on the raw data of this matrix so need to protect access
        for row in 0..self.block_size {
            let global_index = (row + start_row) * self.cols + (start_col);
            for col in 0..self.block_size {
                raw[global_index + col] += mat[row * self.block_size + col];
            }
        }
    }
}
async fn get_sub_mat(mat: &SubMatrix, sub_mat: &LamellarLocalMemoryRegion<f32>) {
    let start_row = mat.row_block * mat.block_size;
    let start_col = mat.col_block * mat.block_size;
    let sub_mat_slice = unsafe { sub_mat.as_mut_slice().unwrap() };
    sub_mat_slice[sub_mat.len() - 1] = f32::NAN;
    for row in 0..mat.block_size {
        let offset = (row + start_row) * mat.cols + (start_col);
        let data = sub_mat.sub_region(row * mat.block_size..(row + 1) * mat.block_size);
        unsafe {
            mat.mat.get(mat.pe, offset, &data);
        }
    }
    while sub_mat_slice[sub_mat.len() - 1].is_nan() {
        // async_std::task::yield_now().await;
        std::thread::yield_now();
    }
}



#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct NaiveMM {
    a: SubMatrix, // will always be local
    b: SubMatrix, // can possibly be remote
    c: SubMatrix, // will always be remote
    block_size: usize,
}

#[lamellar::am]
impl LamellarAM for NaiveMM {
    fn exec() {
        let a = lamellar::world.alloc_local_mem_region(self.a.block_size * self.a.block_size); //the tile for the A matrix
        let b = lamellar::world.alloc_local_mem_region(self.b.block_size * self.b.block_size); //the tile for the B matrix
        let b_fut = get_sub_mat(&self.b, &b); //b is remote so we will launch "gets" for this data first
        let a_fut = get_sub_mat(&self.a, &a);
        let a_b_fut = future::join(a_fut, b_fut);
        a_b_fut.await;
        let mut res = vec![f32::NAN; a.len()]; //the tile for the result of the MM
        let block_size = self.block_size;
        unsafe {
            sgemm(
                block_size,
                block_size,
                block_size,
                1.0,
                a.as_ptr().unwrap(),
                block_size as isize,
                1,
                b.as_ptr().unwrap(),
                block_size as isize,
                1,
                0.0,
                res.as_mut_ptr(),
                block_size as isize,
                1,
            );
        }
        self.c.add_mat(&res); // add the tile to the global result
        lamellar::world.free_local_memory_region(a);
        lamellar::world.free_local_memory_region(b);
    }
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

    let a = world.alloc_shared_mem_region::<f32>((m * n) / num_pes);
    let b = world.alloc_shared_mem_region::<f32>((n * p) / num_pes);
    let c = world.alloc_shared_mem_region::<f32>((m * p) / num_pes);
    unsafe {
        let mut cnt = 0.0;
        for elem in a.as_mut_slice().unwrap() {
            *elem = cnt;
            cnt += 1.0;
        }
        for elem in b.as_mut_slice().unwrap() {
            *elem = 2.0;
        }
        for elem in c.as_mut_slice().unwrap() {
            *elem = 0.0;
        }
    }

    let num_gops = ((2 * dim * dim * dim) - dim * dim) as f64 / 1_000_000_000.0; // accurate for square matrices

    if my_pe == 0 {
        println!("starting");
    }
    let mut tot_mb = 0.0f64;
    for bs in [2000, 1000, 500].iter() {
        let block_size = *bs;
        let m_blocks = m / block_size;
        let n_blocks = n / block_size;
        let p_blocks = p / block_size;
        let a_pe_row_blks = m_blocks / num_pes;
        let a_pe_col_blks = n_blocks;
        let b_pe_row_blks = n_blocks / num_pes;
        let b_pe_col_blks = p_blocks;

        //A iteration:
        let mut tasks = 0;
        let start = std::time::Instant::now();
        for i in 0..a_pe_row_blks {
            //iterate over rows of A, (all tiles in a row are local)
            for j in 0..b_pe_col_blks {
                // need to iterate over every column of B (some tiles will be nonlocal)
                let c_block =
                    SubMatrix::new("C".to_owned(), c.clone(), my_pe, m, n, i, j, block_size);
                for k in 0..a_pe_col_blks {
                    //iterate over cols of a row of A (all are local), likewise iterate over rows of col of B (some are non local)
                    let a_block =
                        SubMatrix::new("A".to_owned(), a.clone(), my_pe, m, n, i, k, block_size); //an entire row of A is local
                    let b_block = SubMatrix::new(
                        "B".to_owned(),
                        b.clone(),
                        k / b_pe_row_blks,
                        n,
                        p,
                        k % b_pe_row_blks,
                        j,
                        block_size,
                    ); //B is distributed
                    world.exec_am_pe(
                        my_pe,
                        NaiveMM {
                            a: a_block,
                            b: b_block,
                            c: c_block.clone(),
                            block_size: block_size,
                        },
                    );
                    tasks += 1;
                }
            }
        }
        world.wait_all();
        world.barrier();
        let elapsed = start.elapsed().as_secs_f64();
        if my_pe == 0 {
            println!(
                "blocksize {:?} elapsed {:?} Gflops: {:?} MB: {:?} ({:?}, {:?}) tasks {:?}",
                block_size,
                elapsed,
                num_gops / elapsed,
                world.MB_sent()[0] - tot_mb,
                world.MB_sent()[0],
                tot_mb,
                tasks
            );
        }
        tot_mb = world.MB_sent()[0];
    }
}
