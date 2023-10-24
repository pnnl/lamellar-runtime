/// ------------Lamellar Example: AM no return-------------------------
/// This example highlights how to create a Lamellar Active message
/// the active message consists of a single input value (the pe id of the originating pe)
/// when executed it will print the active message data and return no data
/// it tests executing the AM localy, executing remotely, and executing on all PEs
/// finally it performs a ring like pattern where each pe sends an AM to its right neigbor (wrapping to 0 for the last pe)
/// --------------------------------------------------------------------
// use lamellar::ActiveMessaging;
use lamellar::active_messaging::prelude::*;
// use lamellar::{Backend, SchedulerType};

// use tracing_flame::FlameLayer;
// use tracing_subscriber::{fmt, prelude::*, registry::Registry};

//----------------- Active message returning nothing-----------------//
#[lamellar::AmData(Debug, Clone)]
struct AmNoReturn {
    my_pe: usize,
    #[AmGroup(static)]
    test_var: u16,
}

#[lamellar::am]
impl LamellarAM for AmNoReturn {
    async fn exec(self) {
        println!(
            "\tin AmNoReturn {:?} on pe {:?} of {:?} ({:?})",
            self,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap()
        );
        println!("\t{:?} {:?} leaving", self.my_pe, self.test_var);
    }
}

// fn setup_global_subscriber() -> impl Drop {
//     let fmt_layer = fmt::Layer::default();

//     let (flame_layer, _guard) = FlameLayer::with_file("./tracing.folded").unwrap();
//     let flame_layer = flame_layer.with_threads_collapsed(true);

//     let subscriber = Registry::default().with(fmt_layer).with(flame_layer);

//     tracing::subscriber::set_global_default(subscriber).expect("Could not set global default");
//     _guard
// }

fn main() {
    // let subscriber = tracing_subscriber::FmtSubscriber::builder()
    //     .with_max_level(Level::TRACE)
    //     .init();
    // let _guard = setup_global_subscriber();
    let world = LamellarWorldBuilder::new()
        //.with_lamellae(Default::default()) //if enable-rofi feature is active default is rofi, otherwise local
        //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend to rofi, with the default provider
        //.with_lamellae( Backend::Local )
        // .with_scheduler(lamellar::SchedulerType::WorkStealing) //currently the only type of thread scheduler
        .build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    // let _guard = setup_global_subscriber();
    world.barrier();
    let am = AmNoReturn {
        my_pe: my_pe,
        test_var: 1000,
    };
    if my_pe == 0 {
        println!("---------------------------------------------------------------");
        println!("Testing local am no return");
        let res = world.block_on(world.exec_am_pe(my_pe, am.clone()));
        assert_eq!(res, ());
        println!("no return result: {:?}", res);
        println!("-----------------------------------");
        println!("Testing remote am no return");
        // for i in 0..1 {
        //     world.exec_am_pe(num_pes - 1, am.clone());
        // }
        // world.wait_all();
        let res = world.block_on(world.exec_am_pe(num_pes - 1, am.clone()));
        assert_eq!(res, ());
        println!("no return result: {:?}", res);
        println!("-----------------------------------");
        println!("Testing all am no return");
        println!("[{:?}] exec on all", my_pe);
        let res = world.block_on(world.exec_am_all(am.clone()));
        assert!(res.iter().all(|x| *x == ()));
        println!("no return result: {:?}", res);
        println!("---------------------------------------------------------------");

        println!("Task Group---------------------------------------------------------------");

        // let mut am_group = typed_am_group!(AmNoReturn,world.clone());
        // am_group.add_am_all(am.clone());
        // am_group.add_am_pe(0,am.clone());
        // world.block_on(am_group.exec());

        let mut am_group = typed_am_group!(AmNoReturn, world.clone());
        for i in 0..10 {
            am_group.add_am_pe(
                i % num_pes,
                AmNoReturn {
                    my_pe: i,
                    test_var: 10 * (i as u16),
                },
            );
            am_group.add_am_all(AmNoReturn {
                my_pe: i,
                test_var: 10 * (i as u16),
            });
        }
        let res = world.block_on(am_group.exec());
        for r in res.iter() {
            println!("PE[{:?}] return result: {:?}", my_pe, r);
        }
    }

    // println!("---------------------------------------------------------------");
    // println!("Testing ring pattern am no return");
    // let res = world.block_on(world.exec_am_pe((my_pe + 1) % num_pes, am.clone()));
    // assert_eq!(res, ());
    // println!("no return result: {:?}", res);
    // println!("-----------------------------------");
}
