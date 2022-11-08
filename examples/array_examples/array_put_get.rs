use lamellar::array::{DistributedIterator, Distribution, UnsafeArray};
use lamellar::{LamellarMemoryRegion, RemoteMemoryRegion};

fn initialize_array(array: &UnsafeArray<usize>) {
    array.dist_iter_mut().for_each(|x| *x = 0);
    array.wait_all();
    array.barrier();
}

fn initialize_mem_region(memregion: &LamellarMemoryRegion<usize>) {
    unsafe {
        let mut i = 0; //(len_per_pe * my_pe as f32).round() as usize;
        for elem in memregion.as_mut_slice().unwrap() {
            *elem = i;
            i += 1
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    world.block_on(async {
        let _num_pes = world.num_pes();
        let _my_pe = world.my_pe();
        let total_len = args
            .get(1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or_else(|| 100);

        let block_array =
            UnsafeArray::<usize>::new(world.team(), total_len, Distribution::Block).into(); //convert into abstract LamellarArray
        let cyclic_array =
            UnsafeArray::<usize>::new(world.team(), total_len, Distribution::Cyclic).into();
        let shared_mem_region = world.alloc_shared_mem_region(total_len).into(); //Convert into abstract LamellarMemoryRegion
        let local_mem_region = world.alloc_one_sided_mem_region(total_len).into();
        initialize_array(&block_array);
        initialize_array(&cyclic_array);
        initialize_mem_region(&shared_mem_region);
        initialize_mem_region(&local_mem_region);
        println!("data initialized");
        world.barrier();

        // puts/gets with memregions
        let start = std::time::Instant::now();
        block_array.put(0, &shared_mem_region).await; //uses the local data of the shared memregion
        block_array.put(0, &local_mem_region).await;
        cyclic_array.put(0, &shared_mem_region).await;
        cyclic_array.put(0, &local_mem_region).await;
        println!("put elapsed {:?}", start.elapsed().as_secs_f64());
        world.barrier();
        // can use subregions
        unsafe {
            let start = std::time::Instant::now();
            block_array.get_unchecked(0, shared_mem_region.sub_region(0..total_len / 2)); //uses local data of the shared memregion
            block_array.get_unchecked(0, local_mem_region.sub_region(0..total_len / 2));
            println!("get_unchecked elapsed {:?}", start.elapsed().as_secs_f64());
        }
        let start = std::time::Instant::now();

        cyclic_array
            .get(0, shared_mem_region.sub_region(0..total_len / 2))
            .await;
        cyclic_array
            .get(0, local_mem_region.sub_region(0..total_len / 2))
            .await;

        println!("get elapsed {:?}", start.elapsed().as_secs_f64());
        world.barrier();
        // puts/gets using single values
        block_array.put(total_len - 1, &12345).await;
        cyclic_array.put(total_len - 1, &12345).await;
        world.barrier();
    });

    // in the future will be able to use and input/output :
    // LamellarArrays
    // Slices
    // Vactores
}
