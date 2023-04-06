/// ------------Lamellar Example: AM returm usize-------------------------
/// This example highlights how to create a Lamellar Active message
/// with multiple input types and returns a usize.
/// it tests executing the AM localy, executing remotely, and executing on all PEs
/// --------------------------------------------------------------------
use lamellar::active_messaging::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
// use lamellar::{Backend, SchedulerType};

//----------------- Active message returning data--------------------//
#[lamellar::AmData(Debug, Clone)]
struct AmReturnUsize {
    val1: usize,
}

#[lamellar::am]
impl LamellarAM for AmReturnUsize {
    async fn exec(&self) -> usize {
        // println!("{:?} {:?}",lamellar::current_pe,self.val1);
        self.val1
    }
}
//-------------------------------------------------------------------//

fn main() {
    let world = lamellar::LamellarWorldBuilder::new()
        // .with_lamellae(Default::default()) //if enable-rofi feature is active default is rofi, otherwise local
        //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend
        //.with_lamellae( Backend::Local )
        // .with_scheduler(SchedulerType::WorkStealing) //currently the only type of thread scheduler
        .build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();

    let mut rng: StdRng = SeedableRng::seed_from_u64(10);

    if my_pe == 0 {
        let num_ams = 1_000_000;
        // println!("---------------------------------------------------------------");
        // println!("Testing local am");
        // let res = world.block_on(world.exec_am_pe(my_pe, am.clone()));
        // assert_eq!(res, my_pe);
        // println!("PE[{:?}] return result: {:?}", my_pe, res);
        // println!("-----------------------------------");
        // println!("Testing remote am");
        // let res = world.block_on(world.exec_am_pe(num_pes - 1, am.clone()));
        // assert_eq!(res, num_pes - 1);
        // println!("PE[{:?}] return result: {:?}", my_pe, res);
        // println!("-----------------------------------");
        // println!("Testing all am");
        // let res = world.block_on(world.exec_am_all(am));
        // assert_eq!(res, (0..num_pes).collect::<Vec<usize>>());
        // println!("PE[{:?}] return result: {:?}", my_pe, res);
        // println!("---------------------------------------------------------------");
        // let res = world.block_on(world.exec_am_all(AmReturnUsize{val1: 5}));
        // println!("res: {res:?}");
        let mut ams = typed_am_group!(AmReturnUsize, &world);
        let mut check = vec![];
        for i in 0..num_ams {
            let pe = rng.gen_range(0..(num_pes + 1));
            if pe == num_pes {
                ams.add_am_all(AmReturnUsize { val1: i });
            } else {
                ams.add_am_pe(pe, AmReturnUsize { val1: i });
            }
            check.push((pe, i));
        }
        let results = world.block_on(ams.exec());
        for (pe, i) in check {
            match results.at(i) {
                AmGroupResult::Pe(the_pe, val) => {
                    assert_eq!(pe, the_pe);
                    assert_eq!(i, *val);
                }
                AmGroupResult::All(vals) => {
                    for val in vals {
                        assert_eq!(i, *val);
                    }
                }
            }
        }
        println!("Done!");
    }
}
