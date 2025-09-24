/// ------------Lamellar Example: RDMA get -------------------------
/// this example highlights constructing a lamellar memory region
/// and performing an RDMA get of remote data from the region into
/// a local buffer
///----------------------------------------------------------------
use lamellar::memregion::prelude::*;

const SHARED_MEM_REGION_LEN: usize = 100;

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
        let shared_mem_region = world
            .alloc_shared_mem_region::<u8>(SHARED_MEM_REGION_LEN)
            .block();
        let shared_mem_region_slice = unsafe { shared_mem_region.as_slice() }; //we can unwrap because we know shared_mem_region is local
                                                                               // instatiates a local shared_mem_region whos memory is registered with
                                                                               // the underlying network device, so that it can be used
                                                                               // as the src buffer in a put or as the dst buffer in a get
        let data = world.alloc_one_sided_mem_region::<u8>(SHARED_MEM_REGION_LEN);
        let data_slice = unsafe { data.as_mut_slice() }; //we can unwrap because we know data is local
        for elem in data_slice.iter_mut() {
            *elem = my_pe as u8;
        }

        // we can use the local_shared_mem_region to initialize our local portion a shared memory region
        unsafe {
            shared_mem_region.put_buffer(my_pe, 0, data.clone()).block();
        };

        //we can "get" from a remote segment of a shared mem region into a local segment of the shared mem region
        world.barrier();
        if my_pe == 0 {
            println!("-------------- testing single element get --------------");
            let val = unsafe {
                shared_mem_region.get(num_pes - 1, 0).block();
            };
            println!("[{:?}] got {:?}", my_pe, val);

            println!("-------------------------------------------------------");
            println!("-------------- testing get_buffer -------------------");
            let buf = unsafe {
                shared_mem_region
                    .get_buffer(num_pes - 1, 0, SHARED_MEM_REGION_LEN)
                    .block()
            };
            println!("[{:?}] got {:?}", my_pe, buf);
            println!(
                "-------------- testing shared mem region get to shared mem region --------------"
            );
            println!("[{:?}] Before {:?}", my_pe, shared_mem_region_slice);
            unsafe {
                let buffer = LamellarBuffer::from_shared_memory_region(shared_mem_region.clone());
                shared_mem_region
                    .get_into_buffer(num_pes - 1, 0, buffer)
                    .block();
            }

            println!("[{:?}] After {:?}", my_pe, shared_mem_region_slice);
            println!(
                "-------------------------------------------------------------------------------"
            );
            unsafe {
                shared_mem_region.put_buffer(my_pe, 0, data.clone()).block();
            }; //reset our local segment
        }

        world.barrier();

        //we can "get" from  another nodes shared mem region into a local_shared_mem_region
        if my_pe == 0 {
            println!(
                "----------------- testing shared mem region get to local_shared_mem_region ----------------"
            );
            println!("[{:?}] Before {:?}", my_pe, data_slice);
            unsafe {
                let buffer = LamellarBuffer::from_one_sided_memory_region(data.clone());
                let _ = shared_mem_region.get_into_buffer_unmanaged(num_pes - 1, 0, buffer);
            }
            shared_mem_region.wait_all();
            println!("[{:?}] After {:?}", my_pe, data_slice);
            println!(
                "-------------------------------------------------------------------------------"
            );
            unsafe {
                let buffer = LamellarBuffer::from_one_sided_memory_region(data.clone());
                shared_mem_region.get_into_buffer(my_pe, 0, buffer).block();
            } // reset local_shared_mem_region;
        }
        world.barrier();
        if my_pe == 0 {
            println!(
                "--------------- testing local_shared_mem_region put_all to shared mem region --------------"
            );
        }
        world.barrier();
        println!("[{:?}] Before {:?}", my_pe, data_slice);
        world.barrier();

        //stripe pe ids accross all shared mem regions
        let mut buffer = unsafe { LamellarBuffer::from_one_sided_memory_region(data.clone()) };
        for i in 0..SHARED_MEM_REGION_LEN {
            let remaining_buffer = buffer.split_off(1);
            unsafe {
                shared_mem_region.get_into_buffer_unmanaged(i % num_pes, i, buffer);
            };
            buffer = remaining_buffer;
        }

        shared_mem_region.wait_all();

        world.barrier();
        if my_pe == 0 {
            println!("----------------------------------------");
        }
        println!("[{:?}] After {:?}", my_pe, unsafe { data.as_slice() });
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
