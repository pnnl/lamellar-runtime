use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;

async fn initialize_array(array: &UnsafeArray<usize>) {
    unsafe { array.dist_iter_mut().for_each(|x| *x = 0).await };
    array.async_barrier().await;
}

fn initialize_mem_region(memregion: &LamellarMemoryRegion<usize>) {
    unsafe {
        let mut i = 0; //(len_per_pe * my_pe as f32).round() as usize;
        for elem in memregion.as_mut_slice() {
            *elem = i;
            i += 1
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    world.clone().block_on(async move {
        let _num_pes = world.num_pes();
        let my_pe = world.my_pe();
        let total_len = args
            .get(1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or_else(|| 100);

        let block_array =
            UnsafeArray::<usize>::new(world.team(), total_len, Distribution::Block).await;
        let cyclic_array =
            UnsafeArray::<usize>::new(world.team(), total_len, Distribution::Cyclic).await;
        let shared_mem_region = world.alloc_shared_mem_region(total_len).await.into(); //Convert into abstract LamellarMemoryRegion
        let local_mem_region = world.alloc_one_sided_mem_region(total_len).into();
        initialize_array(&block_array).await;
        initialize_array(&cyclic_array).await;
        initialize_mem_region(&shared_mem_region);
        initialize_mem_region(&local_mem_region);
        println!("data initialized");
        world.async_barrier().await;

        // puts/gets with memregions
        unsafe {
            block_array.print();
            world.async_barrier().await;
            println!("PE{my_pe}, smr {:?}", shared_mem_region.as_slice());
            world.async_barrier().await;
            let start = std::time::Instant::now();
            if my_pe == 0 {
                block_array.put(0, &shared_mem_region).await
            }; //uses the local data of the shared memregion
            world.async_barrier().await;
            block_array.print();
            world.async_barrier().await;
            println!("PE{my_pe}, smr {:?}", shared_mem_region.as_slice());
            world.async_barrier().await;
            println!("PE{my_pe}, lmr {:?}", local_mem_region.as_slice());
            world.async_barrier().await;
            if my_pe == 0 {
                block_array.put(0, &local_mem_region).await
            };
            world.async_barrier().await;
            block_array.print();
            println!("PE{my_pe}, lmr {:?}", local_mem_region.as_slice());
            world.async_barrier().await;

            cyclic_array.print();
            world.async_barrier().await;
            if my_pe == 0 {
                cyclic_array.put(0, &shared_mem_region).await
            };
            world.async_barrier().await;
            cyclic_array.print();
            world.async_barrier().await;
            println!("PE{my_pe}, smr {:?}", shared_mem_region.as_slice());
            world.async_barrier().await;
            println!("PE{my_pe}, lmr {:?}", local_mem_region.as_slice());
            world.async_barrier().await;
            if my_pe == 0 {
                cyclic_array.put(0, &local_mem_region).await
            };
            world.async_barrier().await;
            cyclic_array.print();
            println!("put elapsed {:?}", start.elapsed().as_secs_f64());
            world.async_barrier().await;

            initialize_array(&block_array).await;
            initialize_array(&cyclic_array).await;
            // can use subregions

            block_array.print();
            world.async_barrier().await;
            println!("PE{my_pe}, smr {:?}", shared_mem_region.as_slice());
            world.async_barrier().await;
            let start = std::time::Instant::now();
            block_array.print();
            world.async_barrier().await;
            println!("PE{my_pe}, smr {:?}", shared_mem_region.as_slice());
            world.async_barrier().await;
            if my_pe == 0 {
                block_array.get_unchecked(0, shared_mem_region.sub_region(0..total_len / 2))
            }; //uses local data of the shared memregion
            println!("PE{my_pe}, lmr {:?}", local_mem_region.as_slice());
            world.async_barrier().await;
            if my_pe == 0 {
                block_array.get_unchecked(0, local_mem_region.sub_region(0..total_len / 2))
            };
            world.async_barrier().await;
            block_array.print();
            println!("PE{my_pe}, lmr {:?}", local_mem_region.as_slice());
            world.async_barrier().await;
            println!("get_unchecked elapsed {:?}", start.elapsed().as_secs_f64());
        }
        let start = std::time::Instant::now();

        unsafe {
            cyclic_array
                .get(0, shared_mem_region.sub_region(0..total_len / 2))
                .await;
            cyclic_array
                .get(0, local_mem_region.sub_region(0..total_len / 2))
                .await;
        }

        println!("get elapsed {:?}", start.elapsed().as_secs_f64());
        world.async_barrier().await;
        // puts/gets using single values
        unsafe {
            block_array.put(total_len - 1, &12345).await;
            cyclic_array.put(total_len - 1, &12345).await;
        }
        world.async_barrier().await;
    });

    // in the future will be able to use and input/output :
    // LamellarArrays
    // Slices
    // Vactores
}
