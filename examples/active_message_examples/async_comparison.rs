/// ------------Lamellar Example: ASYNC Comparison -------------------------
/// This example highlights Lamellars integration with Rusts async/await framework
/// To show this we implement two active messages where the "work" both messages
/// perform is the sleep for some number of seconds.
/// One active message uses std::thread::sleep which blocks the calling thread
/// the other uses async_std::task::sleep which will not block the calling thread,
/// instead placing the task to the back of a work queue and processing the next one.
/// For this example we recommend setting the LAMELLAR_THREADS envrionment variable to 1.
/// E.g. export LAMELLAR_THREADS=1
/// this will show the blocking vs non-blocking nature of the the two active message types
/// --------------------------------------------------------------------
use lamellar::active_messaging::prelude::*;

use std::time::{Duration, Instant};

#[lamellar::AmData(Clone)]
struct StdSleepAM {
    secs: u64,
}

#[lamellar::am]
impl LamellarAM for StdSleepAM {
    async fn exec(self) {
        println!(
            "\tsleeping for {:?} sec on pe: {:?}",
            self.secs,
            lamellar::current_pe
        );
        let timer = Instant::now();
        std::thread::sleep(Duration::from_secs(self.secs));
        println!(
            "\tpe: {:?} actually slept for {:?}",
            lamellar::current_pe,
            timer.elapsed().as_secs_f64()
        );
    }
}

#[lamellar::AmData(Clone)]
struct AsyncSleepAM {
    secs: u64,
}

#[lamellar::am]
impl LamellarAM for AsyncSleepAM {
    async fn exec(self) {
        println!(
            "\tsleeping for {:?} sec on pe: {:?}",
            self.secs,
            lamellar::current_pe
        );
        let timer = Instant::now();
        async_std::task::sleep(Duration::from_secs(self.secs)).await;
        println!(
            "\tpe: {:?} actually slept for {:?}",
            lamellar::current_pe,
            timer.elapsed().as_secs_f64()
        );
    }
}

/*
* This example is indented to highlight the interoperability of Lamellar tasks with Rust async/await capabilities
* to demonstrate the difference between std::sleep and async_std::sleep set the LAMELLAR_THREADS environment variable to 1
* LAMELLAR_THREADS=1 this forces on a single worker thread, meaning each tasks will be executed by that thread.
* the async sleep will place the tasks back in the task pool while they are sleeping, the std sleep will block for the entire time
*/
fn main() {
    match std::env::var("LAMELLAR_THREADS") {
        Ok(num) => if num.parse::<usize>().expect(
            "NOTE: to highlight the effect of async tasks please set LAMELLAR_THREADS env var to 1",
        ) > 1
        {
            println!(" NOTE: to highlight the effect of async tasks please set LAMELLAR_THREADS env var to 1");
        },
        Err(_) => {
            println!(" NOTE: to highlight the effect of async tasks please set LAMELLAR_THREADS env var to 1");
        }
    }
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    world.barrier();

    if my_pe == 0 {
        let std_am = StdSleepAM { secs: 1 };
        let async_am = AsyncSleepAM { secs: 1 };
        println!("---------------------------------------------------------------");
        println!("Testing Std Sleep am");
        let mut timer = Instant::now();
        let mut std_am_group = AmGroup::new(&world);
        // let mut std_am_group = typed_am_group!(StdSleepAM,&world);

        for _i in 0..10 {
            std_am_group.add_am_all(std_am.clone()); //launch multiple tasks asyncronously
        }
        world.block_on(async move {
            std_am_group.exec().await;
        });
        println!(
            "time for std sleep tasks: {:?}",
            timer.elapsed().as_secs_f64()
        );
        println!("-----------------------------------");
        println!("Testing async Sleep am");
        let mut async_am_group = AmGroup::new(&world);
        // let mut async_am_group = typed_am_group!(AsyncSleepAM,&world);
        timer = Instant::now();
        for _i in 0..10 {
            async_am_group.add_am_all(async_am.clone()); //launch multiple tasks asyncronously
        }
        world.block_on(async move {
            async_am_group.exec().await;
        });
        println!(
            "time for async sleep tasks: {:?}",
            timer.elapsed().as_secs_f64()
        );
        println!("---------------------------------------------------------------");
    }
}
