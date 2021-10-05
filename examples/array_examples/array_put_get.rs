use lamellar::array::{Distribution, UnsafeArray};
use lamellar::{ActiveMessaging, LamellarArray, LamellarMemoryRegion, RemoteMemoryRegion};
use std::time::Instant;

fn initialize_array(array: &LamellarArray<usize>) {
    array.for_each_mut(|x| *x = 0);
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

fn put_local(array: &LamellarArray<usize>, memregion: &LamellarMemoryRegion<usize>) {
    array.put(0, memregion);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let num_pes = world.num_pes();
    let my_pe = world.my_pe();
    let total_len = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| 100);

    let block_array =
        UnsafeArray::<usize>::new(world.team(), total_len, Distribution::Block).into(); //convert into abstract LamellarArray
    let cyclic_array =
        UnsafeArray::<usize>::new(world.team(), total_len, Distribution::Cyclic).into();
    let shared_mem_region = world.alloc_shared_mem_region(total_len).into(); //Convert into abstract LamellarMemoryRegion
    let local_mem_region = world.alloc_local_mem_region(total_len).into();
    initialize_array(&block_array);
    initialize_array(&cyclic_array);
    initialize_mem_region(&shared_mem_region);
    initialize_mem_region(&local_mem_region);
    world.barrier();

    // puts/gets with memregions
    block_array.put(0, &shared_mem_region); //uses the local data of the shared memregion
    block_array.put(0, &local_mem_region);
    cyclic_array.put(0, &shared_mem_region);
    cyclic_array.put(0, &local_mem_region);
    // can use subregions
    block_array.get(0, &shared_mem_region.sub_region(0..total_len / 2)); //uses local data of the shared memregion
    block_array.get(0, &local_mem_region.sub_region(0..total_len / 2));
    cyclic_array.get(0, &shared_mem_region.sub_region(0..total_len / 2));
    cyclic_array.get(0, &local_mem_region.sub_region(0..total_len / 2));
    world.barrier();
    // puts/gets using single values
    block_array.put(total_len - 1, 12345);
    cyclic_array.put(total_len - 1, 12345);
    world.barrier();

    // in the future will be able to use and input/output :
    // LamellarArrays
    // Slices
    // Vactores
}
