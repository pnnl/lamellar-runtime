/// ------------Lamellar Example: AM returm Am return usize-------------------------
/// This example highlights how to create a Lamellar Active message
/// with multiple input types which returns as a result another active message.
/// This returned active message is executed automatically upon arrival at the original
/// PE and itself retuns a usize as the final result.
/// it tests executing the AM localy, executing remotely, and executing on all PEs
/// --------------------------------------------------------------------
use lamellar::ActiveMessaging;
// use lamellar::{Backend, SchedulerType};

//--the returned active message that will execute automically upon arrival at the originating node --//
#[lamellar::AmData(Clone, Debug)]
struct ReturnUsizeAM {
    val1: usize,
    val2: String,
}

#[lamellar::am]
impl LamellarAM for ReturnUsizeAM {
    fn exec(&self) -> usize {
        println!(
            "\t\tin ReturnUsizeAM {:?} on pe {:?} ({:?})",
            self,
            lamellar::current_pe,
            hostname::get().unwrap()
        );
        self.val1
    }
}
//-------------------------------------------------------------------//

//--Active message returning an active message that returns nothing--//
#[lamellar::AmData(Clone, Debug)]
struct InitialAM {
    val1: usize,
    val2: String,
}

#[lamellar::am(return_am = "ReturnUsizeAM -> usize")] //we specify as a proc_macro argument the type of AM we are returning
impl LamellarAM for InitialAM {
    fn exec(&self) -> ReturnAM {
        let current_hostname = hostname::get().unwrap().to_string_lossy().to_string();
        println!(
            "\tin  InitialAM {:?} on pe {:?} of {:?} ({:?})",
            self,
            lamellar::current_pe,
            lamellar::num_pes,
            &current_hostname
        );
        ReturnUsizeAM {
            val1: lamellar::current_pe,
            val2: current_hostname,
        }
    }
}
//-------------------------------------------------------------------//

fn main() {
    let world = lamellar::LamellarWorldBuilder::new()
        //.with_lamellae(Default::default()) //if enable-rofi feature is active default is rofi, otherwise local
        //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend
        //.with_lamellae( Backend::Local )
        //.with_scheduler(SchedulerType::WorkStealing) //currently the only type of thread scheduler
        .build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();

    let am = InitialAM {
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
        assert_eq!(
            res,
            (0..num_pes)
                .collect::<Vec<usize>>()
        );
        println!("PE[{:?}] return result: {:?}", my_pe, res);
        println!("---------------------------------------------------------------");
    }
}
