/// ------------Lamellar Example: AM returm Am return usize-------------------------
/// This example highlights how to create a Lamellar Active message 
/// with multiple input types which returns as a result another active message.
/// This returned active message is executed automatically upon arrival at the original
/// PE and itself retuns a usize as the final result.
/// it tests executing the AM localy, executing remotely, and executing on all PEs
/// --------------------------------------------------------------------

use lamellar::{ActiveMessaging};
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

#[lamellar::AmData(Clone, Debug)]
struct ReturnVecAM {
    val1: usize,
    val2: String,
    vec: Vec<usize>,
}

#[lamellar::am]
impl LamellarAM for ReturnVecAM {
    fn exec(&self) -> Vec<usize> {
        // println!("{:?}",self);
        // println!(
        //     "\t\tin ReturnUsizeAM {:?} on pe {:?} ({:?})",
        //     self,
        //     lamellar::current_pe,
        //     hostname::get().unwrap()
        // );
        self.vec.clone()

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
    fn exec(&self) -> ReturnUsizeAM {
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

#[lamellar::AmData(Clone, Debug)]
struct InitialAMVec {
    val1: usize,
    val2: String,
    vec: Vec<usize>,
}

#[lamellar::am(return_am = "ReturnVecAM -> Vec<usize>")] //we specify as a proc_macro argument the type of AM we are returning
impl LamellarAM for InitialAMVec {
    fn exec(&self) -> ReturnVecAM {
        let current_hostname = hostname::get().unwrap().to_string_lossy().to_string();
        ReturnVecAM {
            val1: self.val1,
            val2: current_hostname,
            vec: vec![1;self.val1],
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
        for i in 0..10{
            world.exec_am_all( InitialAMVec { val1: 1, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;i]}); //batch msg ,batch return
            world.exec_am_all( InitialAMVec { val1: 1, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;100000]});//direct msg , batch return
            world.exec_am_all( InitialAMVec { val1: 100000, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;i]}); //batch message, direct return
            world.exec_am_pe( 1,InitialAMVec { val1: 100000, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;100000]}); //direct msg, direct return
        }
        world.wait_all();
        // println!("---------------------------------------------------------------");
        // println!("Testing local am");
        // let res = world.exec_am_pe(my_pe, am.clone()).get();
        // assert_eq!(res, Some(my_pe));
        // println!("PE[{:?}] return result: {:?}", my_pe, res);
        // println!("-----------------------------------");
        // println!("Testing remote am");
        // let res = world.exec_am_pe(num_pes - 1, am.clone()).get();
        // assert_eq!(res, Some(num_pes - 1));
        // println!("PE[{:?}] return result: {:?}", my_pe, res);
        // println!("-----------------------------------");
        // println!("Testing all am");
        // let res = world.exec_am_all(am).get_all();
        // assert_eq!(
        //     res,
        //     (0..num_pes)
        //         .map(|x| Some(x))
        //         .collect::<Vec<Option<usize>>>()
        // );
        // println!("PE[{:?}] return result: {:?}", my_pe, res);
        // println!("---------------------------------------------------------------");
    }
}
