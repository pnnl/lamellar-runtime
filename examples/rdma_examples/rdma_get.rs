/// ------------Lamellar Example: RDMA get -------------------------
/// this example highlights constructing a lamellar memory region
/// and performing an RDMA get of remote data from the region into
/// a local buffer
///----------------------------------------------------------------
use lamellar::RemoteMemoryRegion;

const ARRAY_LEN: usize = 100;

// memory regions are low level (unsafe) abstractions
// upon which we will build safer PGAS abstractions
// we provide APIs for these memory regions but they
// are intended mostly for internal use in the runtime
fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    if num_pes > 1 {
        // instatiates a shared memory region on every PE in world
        // all other pes can put/get into this region
        let array = world.alloc_shared_mem_region::<u8>(ARRAY_LEN);
        let array_slice = unsafe{array.as_slice().unwrap()}; //we can unwrap because we know array is local
                                                     // instatiates a local array whos memory is registered with
                                                     // the underlying network device, so that it can be used
                                                     // as the src buffer in a put or as the dst buffer in a get
        let data = world.alloc_one_sided_mem_region::<u8>(ARRAY_LEN);
        let data_slice = unsafe { data.as_mut_slice().unwrap() }; //we can unwrap because we know data is local
        for elem in data_slice.iter_mut() {
            *elem = my_pe as u8;
        }

        // we can use the local_array to initialize our local portion a shared memory region
        unsafe { array.put(my_pe, 0, data.clone()) };

        //we can "get" from a remote segment of a shared mem region into a local segment of the shared mem region
        world.barrier();
        if my_pe == 0 {
            println!(
                "-------------- testing shared mem region get to shared mem region --------------"
            );
            println!("[{:?}] Before {:?}", my_pe, array_slice);
            unsafe {
                array.get_unchecked(num_pes - 1, 0, array.clone());
            }
            while array_slice[ARRAY_LEN - 1] == my_pe as u8 {
                std::thread::yield_now();
            } // wait for data to show up
            println!("[{:?}] After {:?}", my_pe, array_slice);
            println!(
                "-------------------------------------------------------------------------------"
            );
            unsafe { array.put(my_pe, 0, data.clone()) }; //reset our local segment
        }

        world.barrier();

        //we can "get" from  another nodes shared mem region into a local_array
        if my_pe == 0 {
            println!(
                "----------------- testing shared mem region get to local_array ----------------"
            );
            println!("[{:?}] Before {:?}", my_pe, data_slice);
            unsafe {
                array.get_unchecked(num_pes - 1, 0, data.clone());
            }
            while data_slice[ARRAY_LEN - 1] == my_pe as u8 {
                std::thread::yield_now();
            } // local arrays suppore direct indexing, wait for data to show up
            println!("[{:?}] After {:?}", my_pe, data_slice);
            println!(
                "-------------------------------------------------------------------------------"
            );
            unsafe {
                array.get_unchecked(my_pe, 0, data.clone());
            } // reset local_array;
        }
        world.barrier();
        if my_pe == 0 {
            println!(
                "--------------- testing local_array put_all to shared mem region --------------"
            );
        }
        world.barrier();
        println!("[{:?}] Before {:?}", my_pe, data_slice);
        world.barrier();

        //stripe pe ids accross all shared mem regions
        for i in 0..ARRAY_LEN {
            unsafe { array.get_unchecked(i % num_pes, i, data.sub_region(i..=i)) };
        }

        for i in 0..ARRAY_LEN {
            while (i % num_pes) as u8 != data_slice[i] {
                std::thread::yield_now();
            }
        }

        world.barrier();
        if my_pe == 0 {
            println!("----------------------------------------");
        }
        println!("[{:?}] After {:?}", my_pe, unsafe {data.as_slice()});
        world.barrier();
        if my_pe == 0 {
            println!(
                "-------------------------------------------------------------------------------"
            );
        }
    } else {
        println!("this example is intended for multi pe executions");
    }
}
