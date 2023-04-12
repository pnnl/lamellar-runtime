use lamellar::array::prelude::*;
use lamellar::active_messaging::*;
use rand::Rng;

#[AmData]
struct AddAm{
    array: AtomicArray<usize>,
    indices: Vec<usize>,
}

#[am]
impl LamellarAM for AddAm{
    async fn exec(self) {
        let data = self.array.local_data();
        for i in self.indices.iter(){
            data.at(*i).fetch_add(1);
        }
    }
}



fn main() {
    unsafe {
        let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
            Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
            Err(_) => 10000,                      //+ 1 to account for main thread
        };
        let world = lamellar::LamellarWorldBuilder::new().build();
        let num_pes = world.num_pes();
        let my_pe = world.my_pe();
        let array_size = 1000000;
        let array = AtomicArray::<usize>::new(world.clone(), array_size, Distribution::Block); //non intrinsic atomic, non bitwise
        //create vec of random indices between 0 & 1000000
        let mut rng = rand::thread_rng();
        let mut indices = (0..10_000_000).map(|_| rng.gen_range(0..array_size)).collect::<Vec<_>>();
        let vals = vec![1;10_000_000];

        array.barrier();
        let mut timer = std::time::Instant::now();
        array.batch_add(indices.clone(),1);
        if my_pe == 0 {
            println!("{:?}", timer.elapsed());
        }
        world.wait_all();
        world.barrier();
        if my_pe == 0 {
            println!("{:?}", timer.elapsed());
        }
        println!("{:?}", world.block_on(array.sum()));
        world.barrier();
        timer = std::time::Instant::now();
        let mut bufs = vec![Vec::with_capacity(num_per_batch);num_pes];
        for i in indices.iter(){
            let pe = i % num_pes;
            let index = i / num_pes;
            bufs[pe].push(index);
            if bufs[pe].len() == num_per_batch{
                let mut buf = Vec::with_capacity(num_per_batch);
                std::mem::swap(&mut bufs[pe], &mut buf);
                world.exec_am_pe(pe,AddAm{array: array.clone(), indices: buf});
            }
        }
        for (pe,buf) in bufs.drain(..).enumerate(){
            if buf.len() > 0 {
                world.exec_am_pe(pe,AddAm{array: array.clone(), indices: buf});
            }
        }
        if my_pe == 0 {
            println!("{:?}", timer.elapsed());
        }
        world.wait_all();
        if my_pe == 0 {
            println!("{:?}", timer.elapsed());
        }
        world.barrier();
        if my_pe == 0 {
            println!("{:?}", timer.elapsed());
        }
        println!("{:?}", world.block_on(array.sum()));
        let array = array.into_unsafe();

        world.barrier();
        // let iter = vals.into_iter();
        timer = std::time::Instant::now();
        array.new_add(indices.iter(),(0..10_000_000).map(|_| 1));
        if my_pe == 0 {
            println!("{:?}", timer.elapsed());
        }
        world.wait_all();
        world.barrier();
        if my_pe == 0 {
            println!("{:?}", timer.elapsed());
        }
        println!("{:?}", world.block_on(array.sum()));
    }
}