/// ------------Lamellar Example: AM returm Am -------------------------
/// This example highlights how to create a Lamellar Active message
/// with multiple input types which returns as a result another active message.
/// This returned active message is executed automatically upon arrival at the original
/// PE but does not return any data to the user.
/// it tests executing the AM localy, executing remotely, and executing on all PEs
/// --------------------------------------------------------------------
use lamellar::active_messaging::prelude::*;

//--Active message returning an active message that returns nothing--//
#[lamellar::AmData(Clone, Debug)]
struct InitialAM {
    val1: usize,
    val2: String,
}

#[lamellar::am(return_am = "ReturnAM")] //we specify as a proc_macro argument the type of AM we are returning
impl LamellarAM for InitialAM {
    async fn exec(&self) -> ReturnAM {
        let current_hostname = hostname::get().unwrap().to_string_lossy().to_string();
        println!(
            "\tin  InitialAM {:?} {:?} on pe {:?} of {:?} ({:?})",
            self,
            self.val1,
            lamellar::current_pe,
            lamellar::num_pes,
            &current_hostname
        );
        ReturnAM {
            val1: lamellar::current_pe + 100 * self.val1,
            val2: current_hostname,
        }
    }
}
//-------------------------------------------------------------------//

//--the returned active message that will execute automically upon arrival at the originating node --//
// note that because of the way macros are expanded and evaluated we need to define the returned AM before it is used in another AM
#[lamellar::AmData(Debug)]
struct ReturnAM {
    val1: usize,
    val2: String,
}

#[lamellar::am]
impl LamellarAM for ReturnAM {
    async fn exec(&self) {
        println!(
            "\t\tin ReturnAM {:?} {:?} on pe {:?} ({:?})",
            self,
            self.val1,
            lamellar::current_pe,
            hostname::get().unwrap()
        );
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

    let am = InitialAM {
        val1: my_pe,
        val2: hostname::get().unwrap().to_string_lossy().to_string(),
    };

    if my_pe == 0 {
        println!("---------------------------------------------------------------");
        println!("Testing local am");
        let res = world.block_on(world.exec_am_pe(my_pe, am.clone()));
        assert_eq!(res, ());
        println!("PE[{:?}] return result: {:?}", my_pe, res);
        println!("-----------------------------------");
        println!("Testing remote am");
        let res = world.block_on(world.exec_am_pe(num_pes - 1, am.clone()));
        assert_eq!(res, ());
        println!("PE[{:?}] return result: {:?}", my_pe, res);
        println!("-----------------------------------");
        println!("Testing all am");
        let res = world.block_on(world.exec_am_all(am.clone()));
        assert!(res.iter().all(|x| *x == ()));
        println!("PE[{:?}] return result: {:?}", my_pe, res);
        println!("---------------------------------------------------------------");
        let mut am_group = typed_am_group!(InitialAM, world.clone());
        for i in 0..10 {
            let am = InitialAM {
                val1: i,
                val2: hostname::get().unwrap().to_string_lossy().to_string(),
            };
            am_group.add_am_pe(i % num_pes, am.clone());
            am_group.add_am_all(am.clone());
        }
        let res = world.block_on(am_group.exec());
        for r in res.iter() {
            println!("PE[{:?}] return result: {:?}", my_pe, r);
        }
    }
}
