///----------------------------Lamellar GEMM 2 --------------------------------------
/// Similar to the naive implementation (in naive_gemm.rs) this algorithm
/// performs blockwise (sub matrix) mat mults.
/// the key difference is that this version has a small optimization to
/// reuse a remote block so that it only needs to be transfered once.
/// we launch active messages so that the result of a (blockwise) mat mult
/// is stored to the local portion of the C matrix. That is, we never transfer
/// mat mult results over the network
///
/// matrices use row-wise distribution (i.e. all elements of a row are local to a pe,
/// conversely this means elements of a column are distributed across pes)
///---------------------------------------------------------------------------------
use lamellar::{ActiveMessaging};
use lamellar::{
    LamellarLocalMemoryRegion, LamellarMemoryRegion, RegisteredMemoryRegion, RemoteMemoryRegion,
};
use lazy_static::lazy_static;
use matrixmultiply::sgemm;
use parking_lot::Mutex;

lazy_static! {
    static ref LOCK: Mutex<()> = Mutex::new(());
}

#[lamellar::AmData( Clone, Debug)]
struct SubMatrix {
    name: String,
    mat: LamellarMemoryRegion<f32>,
    pe: usize,
    rows: usize,
    cols: usize,
    row_block: usize,
    col_block: usize,
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
    // println!("getting {:?}",mat);
    for row in 0..mat.block_size {
        let offset = (row + start_row) * mat.cols + (start_col);
        let data = sub_mat.sub_region(row * mat.block_size..(row + 1) * mat.block_size);
        unsafe {
            mat.mat.get(mat.pe, offset, &data);
        }
    }

    while sub_mat_slice[sub_mat.len() - 1].is_nan() {
        async_std::task::yield_now().await;
        // std::thread::yield_now();
    }
}


#[lamellar::AmData( Clone, Debug)]
struct MatMulAM {
    a: SubMatrix,     // a is always local
    b: SubMatrix,     // b is possibly remote
    c: SubMatrix,     // c is always local
    a_pe_rows: usize, //the number of rows in a per pe
    block_size: usize,
}

#[lamellar::am]
impl LamellarAM for MatMulAM {
    fn exec() {
        let b =
            lamellar::world.alloc_local_mem_region::<f32>(self.b.block_size * self.b.block_size);
        get_sub_mat(&self.b, &b).await;
        // we dont actually want to alloc a shared memory region as there is an implicit barrier here
        // introduces sync point and potential for deadlock
        // let b = lamellar::world.alloc_shared_mem_region::<f64>(b_vec.len());
        // unsafe {
        //     b.put(lamellar::current_pe as usize, 0, &b_vec);
        // }

        // let mut reqs = vec![];
        for row in 0..self.a_pe_rows {
            let mut a = self.a.clone();
            a.row_block = row;
            let mut c = self.c.clone();
            c.row_block = row;
            let sub_a = lamellar::world.alloc_local_mem_region::<f32>(a.block_size * a.block_size);
            get_sub_mat(&a, &sub_a).await; //this should be local copy so returns immediately
            do_gemm(&sub_a, &b, c, self.block_size);
            lamellar::world.free_local_memory_region(sub_a);
        }
        lamellar::world.free_local_memory_region(b);
    }
}

fn do_gemm(
    a: &LamellarLocalMemoryRegion<f32>,
    b: &LamellarLocalMemoryRegion<f32>,
    c: SubMatrix,
    block_size: usize,
) {
    // let b = self.b.as_slice();
    let mut res = vec![f32::NAN; a.len()];
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
    c.add_mat(&res);
    // lamellar::world.free_local_memory_region(a);
}

#[lamellar::AmData(Clone, Debug)]
struct CachedMM {
    a: SubMatrix,
    b: LamellarLocalMemoryRegion<f32>,
    c: SubMatrix,
    block_size: usize,
}
#[lamellar::am]
impl LamellarAM for CachedMM {
    fn exec() {
        let a =
            lamellar::world.alloc_local_mem_region::<f32>(self.a.block_size * self.a.block_size);
        get_sub_mat(&self.a, &a).await; //this should be local copy so returns immediately
                                        // let b = self.b.as_slice();
        let block_size = self.block_size;
        let mut res = vec![f32::NAN; a.len()];
        unsafe {
            sgemm(
                block_size,
                block_size,
                block_size,
                1.0,
                a.as_ptr().unwrap(),
                block_size as isize,
                1,
                self.b.as_ptr().unwrap(),
                block_size as isize,
                1,
                0.0,
                res.as_mut_ptr(),
                block_size as isize,
                1,
            );
        }
        self.c.add_mat(&res);
        lamellar::world.free_local_memory_region(a);
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
    if let Ok(size) = std::env::var("LAMELLAR_ROFI_MEM_SIZE") {
        let size = size
            .parse::<usize>()
            .expect("invalid memory size, please supply size in bytes");
        if size < 300 * 1024 * 1024 * num_pes {
            println!("This example requires ~300 MB x Num_PEs of 'local' space, please set LAMELLAR_ROFI_MEM_SIZE env var appropriately ");
            std::process::exit(1);
        }
    } else if 1 * 1024 * 1024 * 1024 < 300 * 1024 * 1024 * num_pes {
        //1GB is the default space allocated for 'local' buffers
        println!("This example requires ~300 MB x Num_PEs of 'local' space, please set LAMELLAR_ROFI_MEM_SIZE env var appropriately ");
        std::process::exit(1);
    }

    let dim = elem_per_pe * num_pes;

    //only doing square matrices currently
    let m = dim; //a & c rows
    let n = dim; // a cols b rows
    let p = dim; // b & c cols

    let a = world.alloc_shared_mem_region::<f32>((m * n) / num_pes);
    let b = world.alloc_shared_mem_region::<f32>((n * p) / num_pes);
    let c = world.alloc_shared_mem_region::<f32>((m * p) / num_pes);
    let c2 = world.alloc_shared_mem_region::<f32>((m * p) / num_pes);
    unsafe {
        let mut cnt = my_pe as f32 * ((m * n) / num_pes) as f32;
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
        for elem in c2.as_mut_slice().unwrap() {
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
        let a_pe_rows = m_blocks / num_pes;
        let a_pe_cols = n_blocks;
        let b_pe_rows = n_blocks / num_pes;
        let b_pe_cols = p_blocks;

        //A iteration:
        let mut tasks = 0;
        let start = std::time::Instant::now();
        for j in 0..b_pe_cols {
            // need to iterate over every column of be
            let c_block = SubMatrix::new("C".to_owned(), c2.clone(), my_pe, m, n, 0, j, block_size);
            let mut reqs = vec![];
            for k in 0..a_pe_cols {
                // iterating over
                //a col == b row
                let a_block =
                    SubMatrix::new("A".to_owned(), a.clone(), my_pe, m, n, 0, k, block_size);
                let b_block = SubMatrix::new(
                    "B".to_owned(),
                    b.clone(),
                    k / b_pe_rows,
                    n,
                    p,
                    k % b_pe_rows,
                    j,
                    block_size,
                );
                reqs.push(world.exec_am_pe(
                    my_pe,
                    MatMulAM {
                        a: a_block,
                        b: b_block,
                        c: c_block.clone(),
                        a_pe_rows: a_pe_rows,
                        block_size: block_size,
                    },
                ));
                tasks += 1;
            }
            // for req in reqs {
            //     req.get();
            // }
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

    // for i in 0..c.as_slice().len(){
    //     // println!("{:?} {:?} {:?}",c.as_slice()[i] , c2.as_slice()[i],i);
    //     assert_eq!(c.as_slice()[i] , c2.as_slice()[i],"i: {:?} ({:?})",i,c.as_slice().len());
    // }

    // for pe in 0..num_pes {
    //     if pe == my_pe {
    //         for row in 0..m/num_pes {
    //             for col in 0..p {
    //                 print!("{:?} ", c2.as_slice()[row * p + col]);
    //             }
    //             println!();
    //         }
    //     }
    //     world.barrier();
    // }

    // for pe in 0..num_pes {
    //     if pe == my_pe {
    //         for row in 0..m/num_pes {
    //             for col in 0..p {
    //                 print!("{:?} ", c.as_slice()[row * p + col]);
    //             }
    //             println!();
    //         }
    //     }
    //     world.barrier();
    // }
    // let start = std::time::Instant::now();
    // unsafe {
    //     dgemm(
    //         m,
    //         n,
    //         p,
    //         1.0,
    //         a.as_slice().as_ptr(),
    //         n as isize,
    //         1,
    //         b.as_slice().as_ptr(),
    //         p as isize,
    //         1,
    //         0.0,
    //         c.as_mut_slice().as_mut_ptr(),
    //         p as isize,
    //         1,
    //     );
    // }
    // let elapsed2 = start.elapsed().as_secs_f64();

    // println!("elapsed2: {:?} Gflops: {:?}",elapsed2, num_gops/elapsed2);
    // // println!("{:?}", c.as_slice());
}
