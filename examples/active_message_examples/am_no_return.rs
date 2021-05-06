/// ------------Lamellar Example: AM no return-------------------------
/// This example highlights how to create a Lamellar Active message
/// the active message consists of a single input value (the pe id of the originating pe)
/// when executed it will print the active message data and return no data
/// it tests executing the AM localy, executing remotely, and executing on all PEs
/// finally it performs a ring like pattern where each pe sends an AM to its right neigbor (wrapping to 0 for the last pe)
/// --------------------------------------------------------------------

use lamellar::{ActiveMessaging};
// use lamellar::{Backend, SchedulerType};

//----------------- Active message returning nothing-----------------//
#[lamellar::AmData(Debug, Clone)]
struct AmNoReturn {
    my_pe: usize,
}

#[lamellar::am]
impl LamellarAM for AmNoReturn {
    fn exec(self) {
        println!(
            "\tin AmNoReturn {:?} on pe {:?} of {:?} ({:?})",
            self,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap()
        );
        println!("\t{:?} leaving", self);
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new()
        //.with_lamellae(Default::default()) //if enable-rofi feature is active default is rofi, otherwise local
        //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend to rofi, with the default provider
        //.with_lamellae( Backend::RofiShm ) //explicity set the lamellae backend to rofi, specifying the shm provider
        //.with_lamellae( Backend::RofiVerbs ) //explicity set the lamellae backend to rofi, specifying the verbs provider
        //.with_lamellae( Backend::Local )
        //.with_scheduler(SchedulerType::WorkStealing) //currently the only type of thread scheduler
        .build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();
    let am = AmNoReturn { my_pe: my_pe };
    // if my_pe == 0 {
    //     println!("---------------------------------------------------------------");
    //     println!("Testing local am no return");
    //     let res = world.exec_am_pe(my_pe, am.clone()).get();
    //     assert_eq!(res, None);
    //     println!("no return result: {:?}", res);
    //     println!("-----------------------------------");
    //     println!("Testing remote am no return");
    //     let res = world.exec_am_pe(num_pes - 1, am.clone()).get();
    //     assert_eq!(res, None);
    //     println!("no return result: {:?}", res);
    //     println!("-----------------------------------");
    //     println!("Testing all am no return");
    //     println!("[{:?}] exec on all", my_pe);
    //     let res = world.exec_am_all(am.clone()).get_all();
    //     assert!(res.iter().all(|x| x.is_none()));
    //     println!("no return result: {:?}", res);
    //     println!("---------------------------------------------------------------");
    // }
    let start  = std::time::Instant::now();
    // std::thread::sleep(std::time::Duration::from_millis(1*my_pe as u64));
    println!("---------------------------------------------------------------");
    println!("Testing ring pattern am no return");
    let res = world.exec_am_pe((my_pe + 1) % num_pes, am.clone()).get();
    // std::thread::sleep(std::time::Duration::from_millis(20000));
    assert_eq!(res, None);
    println!("no return result: {:?}", res);
    println!("-----------------------------------");
    // std::thread::sleep(std::time::Duration::from_millis(1000));
    println!("elapsed {:?}",start.elapsed().as_secs_f64());
}
