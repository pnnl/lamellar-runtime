/// ------------Lamellar Example: RDMA get -------------------------
/// this example highlights constructing a lamellar memory region
/// an then embedding and using it within a Lamellar Active message.
/// Within the AM, the memory handle can be used to get data from the
/// a remote pes or put data into a remote pes. In the example,
/// the handles are used on remote pes to access data on the PE which launched the AM
///----------------------------------------------------------------
use lamellar::array::prelude::*;
use lamellar::memregion::prelude::*;
use tracing_subscriber::{fmt,EnvFilter};
use tracing_subscriber::prelude::*;

const ARRAY_LEN: usize = 100;

#[lamellar::AmData(Clone)]
struct RdmaAM {
    array: UnsafeArray<u8>,
    orig_pe: usize,
    index: usize,
}

#[lamellar::am]
impl LamellarAM for RdmaAM {
    async fn exec(&self) {
        let num_pes = lamellar::num_pes;
        let max_i =
            unsafe { std::cmp::min(self.array.local_as_slice().len(), self.index + num_pes) };
        println!(
            "\t({},{}) in RdmaAM on pe {:?}, originating from pe {:?} with index {} and max_i {}",
            self.orig_pe,
            self.index,
            lamellar::current_pe,
            self.orig_pe,
            self.index,
            max_i
        );
        println!("\tlocal segement of array: {:?}..", unsafe {
            &self.array.local_as_slice()[self.index..max_i]
        });

        //get the original nodes data
        let local = lamellar::world.alloc_one_sided_mem_region::<u8>(ARRAY_LEN);
        println!(
            "({},{}) local region allocated at addr {:?}",
            self.orig_pe,
            self.index,
            unsafe { local.as_ptr() }
        );
        let local_slice = unsafe { local.as_mut_slice() };
        local_slice[ARRAY_LEN - 1] = num_pes as u8;
        unsafe {
            self.array.get(0, &local).await;
        }

        let my_index = self.index * num_pes + lamellar::current_pe;
        println!("\t({},{}) current view of remote segment on pe {:?}: {:?}..{:?}\n\tpe: {:?} updating index {:?} on pe  {:?}", self.orig_pe,
        self.index,self.orig_pe, &local_slice[self.index..max_i], &local_slice[local_slice.len()-max_i..],lamellar::current_pe, my_index, self.orig_pe);

        //update an element on the original node
        local_slice[0] = lamellar::current_pe as u8;
        unsafe {
            self.array.put_buffer(my_index, &local.sub_region(0..=0)).await;
        }
    }
}

// memory regions are low level (unsafe) abstractions
// upon which we will build safer PGAS abstractions
// we provide APIs for these memory regions but they
// are intended mostly for internal use in the runtime
//
// SharedMemoryRegions are serializable and can be transfered
// as part of a LamellarAM
fn main() {
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().with_thread_ids(true).with_file(true).with_line_number(true).with_level(true))
        .init();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("creating array");
    let array = UnsafeArray::<u8>::new(world.team(), ARRAY_LEN, Distribution::Block).block();
    println!("creating memregion");
    let local_mem_region = world.alloc_one_sided_mem_region::<u8>(ARRAY_LEN);
    println!("about to initialize array");
    array.print();
    if my_pe == 0 {
        unsafe {
            for i in local_mem_region.as_mut_slice() {
                *i = 255_u8;
            }
        }
        unsafe { array.put_buffer(0, &local_mem_region).block() };
    }

    array.print();
    for i in unsafe { array.local_as_slice() } {
        while *i != 255_u8 {
            std::thread::yield_now();
        }
    }
    if my_pe == 0 {
        println!("------------------------------------------------------------");
    }
    world.barrier();
    println!("about to free mem region");
    drop(local_mem_region);
    println!("freed mem region");
    println!("[{:?}] Before {:?}", my_pe, unsafe {
        array.local_as_slice()
    });
    world.barrier();
    if my_pe == 0 {
        println!("------------------------------------------------------------");
    }
    world.barrier();
    let mut index = 0;
    // while index < ARRAY_LEN / num_pes {
    if my_pe == 0 {
        let _ = world
            .exec_am_pe(1,RdmaAM {
                array: array.clone(),
                orig_pe: my_pe,
                index: index,
            })
            .spawn();
        index += 1;
    }

    world.wait_all();
    world.barrier();
    println!("[{:?}] after {:?}", my_pe, unsafe {
        array.local_as_slice()
    });
    world.barrier();
    array.print();
    if my_pe == 0 {
        println!("------------------------------------------------------------");
    }
    world.barrier();

    if my_pe == 0 {
        let sum = unsafe { array.sum().block() };
        println!("sum: {:?}", sum);
        println!("------------------------------------------------------------");
    }
}
