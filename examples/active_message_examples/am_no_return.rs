use lamellar::{ActiveMessaging, LamellarAM};
// use lamellar::{Backend, SchedulerType};

//----------------- Active message returning nothing-----------------//
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
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
        //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend
        //.with_lamellae( Backend::Local )
        //.with_scheduler(SchedulerType::WorkStealing) //currently the only type of thread scheduler
        .build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();
    let am = AmNoReturn { my_pe: my_pe };
    if my_pe == 0 {
        println!("---------------------------------------------------------------");
        println!("Testing local am no return");
        let res = world.exec_am_pe(my_pe, am.clone()).get();
        assert_eq!(res, None);
        println!("no return result: {:?}", res);
        println!("-----------------------------------");
        println!("Testing remote am no return");
        let res = world.exec_am_pe(num_pes - 1, am.clone()).get();
        assert_eq!(res, None);
        println!("no return result: {:?}", res);
        println!("-----------------------------------");
        println!("Testing all am no return");
        println!("[{:?}] exec on all", my_pe);
        let res = world.exec_am_all(am.clone()).get_all();
        assert!(res.iter().all(|x| x.is_none()));
        println!("no return result: {:?}", res);
        println!("---------------------------------------------------------------");
    }

    println!("---------------------------------------------------------------");
    println!("Testing ring pattern am no return");
    let res = world.exec_am_pe((my_pe + 1) % num_pes, am.clone()).get();
    assert_eq!(res, None);
    println!("no return result: {:?}", res);
    println!("-----------------------------------");
}
