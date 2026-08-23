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

//----------------- Active message returning nothing-----------------//
#[lamellar::AmData(Debug, Clone, AmGroup)]
struct AmNoReturn {
    my_pe: usize,
    // #[AmGroup(static)]
    test_var: u32,
}

#[lamellar::am(AmGroup)]
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
        test_am_no_return();
        test_am_no_return_async().await;
    }
}

#[lamellar::prof]
fn test_am_no_return() {
    println!("This is a test function for AmNoReturn");
}

#[lamellar::prof]
async fn test_am_no_return_async() {
    println!("This is a test async function for AmNoReturn");
    std::thread::sleep(std::time::Duration::from_millis(1));
    async_std::task::yield_now().await;
    std::thread::sleep(std::time::Duration::from_millis(2));
}

#[lamellar::main]
fn main() {
    let start = std::time::Instant::now();
    let world = LamellarWorldBuilder::new()
        //.with_lamellae(Default::default()) //if enable-rofi feature is active default is rofi, otherwise local
        //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend to rofi, with the default provider
        //.with_lamellae( Backend::Local )
        // .with_scheduler(lamellar::SchedulerType::WorkStealing) //currently the only type of thread scheduler
        .build();
    println!("World built in {:?}", start.elapsed());
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();
    // let _guard = setup_global_subscriber();
    let start = std::time::Instant::now();
    world.barrier();
    println!("World barriered in {:?}", start.elapsed());
    if my_pe == 0 {
        println!("---------------------------------------------------------------");
        println!("Testing local am no return");
        // we can use exec_am_pe which returns a lazy future that we can block on to get the result
        let res = world
            .exec_am_pe(
                my_pe,
                AmNoReturn {
                    my_pe: my_pe,
                    test_var: 0,
                },
            )
            .block();
        assert_eq!(res, ());
        println!("no return result: {:?}", res);
        println!("-----------------------------------");
        // we can also use spawn_am_pe which eagerly executes the am and returns a future that we can block on to get the result
        let res = world
            .spawn_am_pe(
                num_pes - 1,
                AmNoReturn {
                    my_pe: my_pe,
                    test_var: 1,
                },
            )
            .block();
        assert_eq!(res, ());
        println!("no return result: {:?}", res);
        println!("-----------------------------------");
        println!("Testing all am no return");
        println!("[{:?}] exec on all", my_pe);
        let res = world
            .exec_am_all(AmNoReturn {
                my_pe: my_pe,
                test_var: 2,
            })
            .block();
        assert!(res.iter().all(|x| *x == ()));
        println!("no return result: {:?}", res);
        println!("-----------------------------------");
        println!("Testing spawn all am no return");
        println!("[{:?}] spawn on all", my_pe);
        let res = world
            .spawn_am_all(AmNoReturn {
                my_pe: my_pe,
                test_var: 2,
            })
            .block();
        assert!(res.iter().all(|x| *x == ()));
        println!("no return result: {:?}", res);
        println!("---------------------------------------------------------------");

        println!("Task Group---------------------------------------------------------------");

        let task_group = LamellarTaskGroup::new(world.clone());
        for i in 1..=10 {
            println!("[{:?}] starting task group loop {}", my_pe, i);
            task_group
                .exec_am_pe(
                    i % num_pes,
                    AmNoReturn {
                        my_pe: i,
                        test_var: 10 * (i as u32),
                    },
                )
                .block();
            task_group
                .spawn_am_pe(
                    i % num_pes,
                    AmNoReturn {
                        my_pe: i,
                        test_var: 10 * (i as u32),
                    },
                )
                .block();
            task_group
                .exec_am_all(AmNoReturn {
                    my_pe: i,
                    test_var: 100 * (i as u32),
                })
                .block();
            task_group
                .spawn_am_all(AmNoReturn {
                    my_pe: i,
                    test_var: 100 * (i as u32),
                })
                .block();
            println!("[{:?}] finished task group loop {}", my_pe, i);
        }
        println!("Typed Am Group---------------------------------------------------------------");

        let mut am_group = typed_am_group!(AmNoReturn, world.clone());
        for i in 1..=10 {
            am_group.add_am_pe(
                i % num_pes,
                AmNoReturn {
                    my_pe: i,
                    test_var: 1000 * (i as u32),
                },
            );
            am_group.add_am_all(AmNoReturn {
                my_pe: i,
                test_var: 10000 * (i as u32),
            });
        }
        let res = world.block_on(am_group.exec());
        for r in res.iter() {
            println!("PE[{:?}] return result: {:?}", my_pe, r);
        }
    }
    println!("PE[{:?}] done", my_pe);
}
