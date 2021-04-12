use lamellar::{ActiveMessaging, LamellarAM};

use std::time::{Duration, Instant};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct StdSleepAM {
    secs: u64,
}

#[lamellar::am]
impl LamellarAM for StdSleepAM {
    fn exec(self) {
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

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct AsyncSleepAM {
    secs: u64,
}

#[lamellar::am]
impl LamellarAM for AsyncSleepAM {
    fn exec(self) {
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
        for _i in 0..10 {
            world.exec_am_all(std_am.clone()); //launch multiple tasks asyncronously
        }
        world.wait_all();
        println!(
            "time for std sleep tasks: {:?}",
            timer.elapsed().as_secs_f64()
        );
        println!("-----------------------------------");
        println!("Testing async Sleep am");
        timer = Instant::now();
        for _i in 0..10 {
            world.exec_am_all(async_am.clone()); //launch multiple tasks asyncronously
        }
        world.wait_all();
        println!(
            "time for async sleep tasks: {:?}",
            timer.elapsed().as_secs_f64()
        );
        println!("---------------------------------------------------------------");
    }
}
