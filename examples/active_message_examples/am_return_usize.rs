/// ------------Lamellar Example: AM returm usize-------------------------
/// This example highlights how to create a Lamellar Active message
/// with multiple input types and returns a usize.
/// it tests executing the AM localy, executing remotely, and executing on all PEs
/// --------------------------------------------------------------------
use lamellar::ActiveMessaging;
// use lamellar::{Backend, SchedulerType};

//----------------- Active message returning data--------------------//
#[lamellar::AmData(Debug, Clone)]
struct AmReturnUsize {
    val1: usize,
    val2: String,
}

#[lamellar::am]
impl LamellarAM for AmReturnUsize {
    fn exec(&self) -> usize {
        println!(
            "\tin  AmReturnUsize self: {:?} on pe {:?} of {:?} ({:?})",
            self,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap()
        );
        lamellar::current_pe
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

    let am = AmReturnUsize {
        val1: my_pe,
        val2: hostname::get().unwrap().to_string_lossy().to_string(),
    };

    if my_pe == 0 {
        println!("---------------------------------------------------------------");
        println!("Testing local am");
        let res = world.block_on(world.exec_am_pe(my_pe, am.clone()));
        assert_eq!(res, my_pe);
        println!("PE[{:?}] return result: {:?}", my_pe, res);
        println!("-----------------------------------");
        println!("Testing remote am");
        let res = world.block_on(world.exec_am_pe(num_pes - 1, am.clone()));
        assert_eq!(res, num_pes - 1);
        println!("PE[{:?}] return result: {:?}", my_pe, res);
        println!("-----------------------------------");
        println!("Testing all am");
        let res = world.block_on(world.exec_am_all(am));
        assert_eq!(res, (0..num_pes).collect::<Vec<usize>>());
        println!("PE[{:?}] return result: {:?}", my_pe, res);
        println!("---------------------------------------------------------------");
    }
}
