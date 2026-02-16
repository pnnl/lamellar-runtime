// use lamellar::array::prelude::*;
// use lamellar::active_messaging::prelude::*;
// use rand::seq::SliceRandom;
// use rand::thread_rng;

// #[AmData]
// struct ApplyPePrefix{
//     array: LocalLockArray<usize>,
//     sum: usize,
// }

// #[am]
// impl LamellarAM for ApplyPePrefix {
//     async fn exec(&self) {
//         self.array.write_local_chunks(1).await.for_each(move|mut chunk| {

//             for i in chunk.iter_mut() {
//                 *i += self.sum;
//             }
//         }).await;
//     }
// }

// fn main() {
//     let world = lamellar::LamellarWorldBuilder::new().build();
//     let my_pe = world.my_pe();
//     let num_pes = world.num_pes();
//     let array_len = 100;

//     // the array we want to prefix sum
//     let array = LocalLockArray::<usize>::new(world.team(), array_len, Distribution::Block).block();
//     let permuted_array =AtomicArray::<usize>::new(world.team(), array_len, Distribution::Block).block();

//     //an array to hold the sum of elements on each pe
//     let pe_sums = AtomicArray::<usize>::new(world.team(), num_pes, Distribution::Block).block();

//     //initialize array
//     array.local_iter_mut().for_each(|i| *i = 1).block();
//     array.print();

//     let chunk_size = array.num_elems_local() / world.num_threads_per_pe();
//     let local_chunk_sums = array.write_local_chunks(chunk_size).block().map(|mut chunk| {
//         let mut sum = 0;
//         for i in chunk.iter_mut() {
//             sum += *i;
//             *i = sum;
//         }
//         sum

//     }).collect::<Vec<_>>(Distribution::Block).block();

//     //calculate the local sum for each pe, and store it into local element of pe_sums
//     pe_sums.local_data().at(0).store(local_chunk_sums.iter().sum::<usize>());

//     //calculate the local prefix sums
//     let _ = array.write_local_chunks(chunk_size).block().enumerate().for_each(move |(i,mut chunk)| {
//         let sum = local_chunk_sums[0..i].iter().sum::<usize>();
//         for i in chunk.iter_mut() {
//             *i += sum;
//         }
//     }).spawn(); //using a safe array we dont actually care if this finishes before we move on

//     pe_sums.barrier();// ensure all pes have writen to pe_sums

//     //calculate the pe prefix sums to reduce communication we only do this on pe 0
//     if my_pe == 0{
//         let mut sum = 0;
//         for (pe, pe_sum) in pe_sums.onesided_iter().into_iter().enumerate().skip(1){
//             sum += pe_sum;

//             // this following is a bit inefficient as have to send the indices for each batch_add but simple to use the array api
//             // let mut  pe_indices = array.first_global_index_for_pe(pe).unwrap()..=array.last_global_index_for_pe(pe).unwrap();
//             // let _ = array.batch_add(&mut pe_indices as &mut dyn Iterator<Item=usize>, sum).spawn();

//             //alteratively we can do this with an AM with much less overhead
//             let _ = world.exec_am_pe(pe,ApplyPePrefix {
//                 array: array.clone(),
//                 sum,
//             }).spawn();
//         }
//     }
//     world.wait_all();
//     world.barrier();

//     array.print();

//     let permuted_array =AtomicArray::<usize>::new(world.team(), array_len, Distribution::Block).block();
//     let mut  pe_indices = (array.first_global_index_for_pe(my_pe).unwrap()..=array.last_global_index_for_pe(my_pe).unwrap()).collect::<Vec<_>>();
//     let mut local_perm_indices = (0..array.num_elems_local()).collect::<Vec<_>>();
//     let mut rng = thread_rng();

//     // Shuffle the vector
//     pe_indices.shuffle(&mut rng);
//     local_perm_indices.shuffle(&mut rng);

//     // let permuted_array_clone = permuted_array.clone();
//     // array.write_local_chunks(chunk_size).block().enumerate().for_each(move |(i,chunk)| {
//     //     // if we know the permutation is local
//     //     // let permuted_local = permuted_array_clone.local_data();
//     //     // for (p_i, elem) in local_perm_indices[i*chunk_size..std::cmp::min((i+1)*chunk_size,local_perm_indices.len())].iter().zip(chunk.iter()){
//     //     //     permuted_local.at(*p_i).store(*elem);
//     //     // }

//     //     // if the permute may contain remote ops -- apply permute to each element individually
//     //     // for (p_i,elem) in pe_indices[i*chunk_size..std::cmp::min((i+1)*chunk_size,pe_indices.len())].iter().zip(chunk.iter()){
//     //     //    let _ = permuted_array_clone.store(*p_i, *elem).spawn();
//     //     // }

//     //     // the above is pretty slow as single element operations are currently not optimized
//     //     // instead we can use the batch store operation
//     //     let  permuted_indices = pe_indices[i*chunk_size..std::cmp::min((i+1)*chunk_size,pe_indices.len())].iter().map(|e| *e).collect::<Vec<_>>();
//     //     let _ = permuted_array_clone.batch_store(permuted_indices, chunk.as_ref()).spawn();

//     // }).block();
//     // world.wait_all();
//     // world.barrier();
//     // permuted_array.print();

//     //likely the best though would be simply do a batch store of the local data, using the permuted indices
//     let local_data = array.read_local_data().block();
//     let _ = permuted_array.batch_store(pe_indices, &local_data).spawn();
//     permuted_array.print();
// }

use std::time::Instant;

use lamellar::array::prelude::*;
//use lamellar::array::Distribution;

#[lamellar::AmData(
    Default,
    Debug,
    ArrayOps(Arithmetic, CompExEps, Shift),
    PartialEq,
    PartialOrd
)]
struct SortElement {
    key: u64,
    val: u64,
}

// These seem to be necessary in order for it to compile.
impl std::ops::AddAssign for SortElement {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            key: self.key + other.key,
            val: self.val + other.val,
        }
    }
}

impl std::ops::SubAssign for SortElement {
    fn sub_assign(&mut self, other: Self) {
        *self = Self {
            key: self.key - other.key,
            val: self.val - other.val,
        }
    }
}

impl std::ops::Sub for SortElement {
    type Output = Self;
    fn sub(self, other: Self) -> Self {
        Self {
            key: self.key - other.key,
            val: self.val - other.val,
        }
    }
}

impl std::ops::MulAssign for SortElement {
    fn mul_assign(&mut self, other: Self) {
        *self = Self {
            key: self.key * other.key,
            val: self.val * other.val,
        }
    }
}

impl std::ops::DivAssign for SortElement {
    fn div_assign(&mut self, other: Self) {
        *self = Self {
            key: self.key / other.key,
            val: self.val / other.val,
        }
    }
}

impl std::ops::RemAssign for SortElement {
    fn rem_assign(&mut self, other: Self) {
        *self = Self {
            key: self.key % other.key,
            val: self.val % other.val,
        }
    }
}

impl std::ops::ShlAssign for SortElement {
    fn shl_assign(&mut self, other: Self) {
        self.key <<= other.key;
        self.val <<= other.val;
    }
}

impl std::ops::ShrAssign for SortElement {
    fn shr_assign(&mut self, other: Self) {
        self.key >>= other.key;
        self.val >>= other.val;
    }
}

/*
fn global_shuffle(A: &UnsafeArray::<SortElement>,
                  B: &UnsafeArray::<SortElement>,
                  world: &LamellarWorld,
                  n_per_task: usize) {
    // Permute elements of A, storing the result into B
    // The actual sort benchmark will use the current key and
    // saved count information for the current task (which can
    // be read back using 'tid'), in order to
    // compute the destination index.
    unsafe {
        let _ =
          A.local_chunks(n_per_task)
             .enumerate().for_each(|(_tid,task_slice)| {
                 //
                 for elt in task_slice.iter() {
                     B.store(elt.key as usize, *elt);
                 }
             });

    }
}*/

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let mut n: usize = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 64);
    let n_tasks_per_pe: usize = world.num_threads_per_pe();
    let n_per_task: usize = usize::div_ceil(n, num_pes * n_tasks_per_pe);
    let n_per_pe: usize = n_tasks_per_pe * n_per_task;
    let n_tasks_total = num_pes * n_tasks_per_pe;
    n = n_per_task * n_tasks_total;
    if my_pe == 0 {
        println!("hello from pe {} of {}", my_pe, num_pes);
        println!("there are {} tasks per pe", n_tasks_per_pe);
        println!(
            "n is {} n_per_pe is {} n_per_task is {}",
            n, n_per_pe, n_per_task
        );
    }

    let A_f = UnsafeArray::<SortElement>::new(world.team(), n, Distribution::Block);
    let B_f = UnsafeArray::<SortElement>::new(world.team(), n, Distribution::Block);
    let A = A_f.block();
    let B = B_f.block();

    // Initialize A. The actual sort benchmark does something more complex
    // (generating random keys). Here we just set the keys to a
    // rotation of the indices.
    unsafe {
        let glob_start = n_per_pe * my_pe;

        let _ = A
            .local_chunks_mut(n_per_task)
            .enumerate()
            .for_each(move |(tid, task_slice)| {
                //  println!("tid {:?} got chunk {:?}", tid, task_slice);
                for (i, elt) in task_slice.iter_mut().enumerate() {
                    let idx = (glob_start + tid * n_per_task + i) as u64;
                    // keys will store the index to store into
                    let k = (idx + 1000) % (n as u64);
                    *elt = SortElement { key: k, val: idx };
                }
                //  println!("tid {:?} ->  chunk {:?}", tid, task_slice);
            })
            .spawn();
        A.wait_all();
    }

    println!("Input to permute:");
    // A.print();

    // Permute elements of A, storing the result into B
    // The actual sort benchmark will use the current key and
    // saved count information for the current task (which can
    // be read back using 'tid'), in order to
    // compute the destination index.

    //global_shuffle(&mut A, &mut B, &world, n_per_task);
    let time = Instant::now();
    let B_clone = B.clone();
    unsafe {
        // let _ =
        //   A.local_chunks(n_per_task)
        //      .enumerate().for_each(move|(_tid,task_slice)| {
        //          //
        //         //  for elt in task_slice.iter() {
        //         //     let _ = B_clone.store(elt.key as usize, *elt).spawn();
        //         //  }
        //         let mut indices = task_slice.iter().map(|e| e.key as usize);
        //         let mut vals = task_slice.iter().map(|e| *e);
        //         let _ = B_clone.batch_store(&mut indices as &mut dyn Iterator<Item=usize>, &mut vals as &mut dyn Iterator<Item = SortElement>).spawn();
        //      }).block();
        let mut indices = A.local_data().iter().map(|e| e.key as usize);
        let mut vals = A.local_data().iter().map(|e| *e);
        B.batch_store(
            &mut indices as &mut dyn Iterator<Item = usize>,
            &mut vals as &mut dyn Iterator<Item = SortElement>,
        )
        .block();
    }

    world.wait_all();
    world.barrier();
    println!("Result of permute: {:?}", time.elapsed().as_secs_f32());
    // B.print();
}
