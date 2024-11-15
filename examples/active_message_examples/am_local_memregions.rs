// use lamellar::{
//     ActiveMessaging, BlockedArch, OneSidedMemoryRegion, RemoteMemoryRegion, StridedArch,
// };
use lamellar::active_messaging::prelude::*;
use lamellar::memregion::prelude::*;
use lamellar::{BlockedArch, StridedArch};
use std::time::Instant;

use rand::distributions::{Distribution, Uniform};

// const ARRAY_LEN: usize = 1 * 1024 * 1024 * 1024;

#[lamellar::AmData(Clone, Debug)]
struct DataAM {
    array: OneSidedMemoryRegion<u8>,
    depth: usize,
    width: usize,
    path: Vec<usize>,
}

#[lamellar::am]
impl LamellarAM for DataAM {
    async fn exec() {
        let mut rng = rand::thread_rng();
        let pes = Uniform::from(0..lamellar::team.num_pes());
        // println!("depth {:?} {:?}",self.depth, self.path);
        let mut path = self.path.clone();
        path.push(lamellar::current_pe);
        if self.depth > 0 {
            for _i in 0..self.width {
                let pe = pes.sample(&mut rng);
                // println!("sending {:?} to {:?}",path,pe);
                let _ = lamellar::team
                    .exec_am_pe(
                        pe,
                        DataAM {
                            array: self.array.clone(),
                            depth: self.depth - 1,
                            width: self.width,
                            path: path.clone(),
                        },
                    )
                    .spawn();
            }
        }
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let array = world.alloc_one_sided_mem_region::<u8>(10);

    let mut rng = rand::thread_rng();
    let pes = Uniform::from(0..num_pes);
    let _width = 10;
    let s = Instant::now();
    // for _i in 0..width {
    //     let pe = pes.sample(&mut rng);
    //     world.exec_am_pe(
    //         pe,
    //         DataAM {
    //             array: array.clone(),
    //             depth: 5,
    //             width: width,
    //             path: vec![my_pe],
    //         },
    //     );
    // }
    world.wait_all();
    world.barrier();
    println!("time: {:?}", s.elapsed().as_secs_f64());
    let first_half_team = world
        .create_team_from_arch(BlockedArch::new(
            0,                                      //start pe
            (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
        ))
        .unwrap(); //okay to unwrap because we are creating a sub_team of the world (i.e. my_pe guaranteed to be in the parent or the subteam)
    let odd_team = world
        .create_team_from_arch(StridedArch::new(
            1,                                      // start pe
            2,                                      // stride
            (num_pes as f64 / 2.0).ceil() as usize, //num pes in team
        ))
        .unwrap(); //okay to unwrap because we are creating a sub_team of the world (i.e. my_pe guaranteed to be in the parent or the subteam)
    let s = Instant::now();
    let width = 5;
    for _i in 0..width {
        let pe = pes.sample(&mut rng) / 2; //since both teams consist of half the number of pes as the world
        let _ = first_half_team
            .exec_am_pe(
                pe,
                DataAM {
                    array: array.clone(),
                    depth: 5,
                    width: width,
                    path: vec![my_pe],
                },
            )
            .spawn();
        let _ = odd_team
            .exec_am_pe(
                pe,
                DataAM {
                    array: array.clone(),
                    depth: 5,
                    width: width,
                    path: vec![my_pe],
                },
            )
            .spawn();
    }
    world.wait_all();
    world.barrier();
    println!("time: {:?}", s.elapsed().as_secs_f64());
}
