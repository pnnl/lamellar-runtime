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
        let num_ams = 1000000;
        // println!("---------------------------------------------------------------");
        let mut ams = typed_am_group!(AmReturnUsize, &world);
        let mut check = vec![];
        for i in 0..num_ams {
            let pe = rng.random_range(0..(num_pes + 1));
            if pe == num_pes {
                ams.add_am_all(AmReturnUsize { val1: i });
            } else {
                ams.add_am_pe(pe, AmReturnUsize { val1: i });
            }
            check.push((pe, i));
        }
        let results = world.block_on(ams.exec());
        for (pe, i) in check {
            // println!("{:?}", results.at(i));
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
