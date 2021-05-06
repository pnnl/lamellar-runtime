///---------------Lamellar Example: Simple PTP---------------------------------
/// a very simple implementation of Orecision Time Protocol for synchronizing clocks in a network
/// highlights using active messages which themselves return other active messages (which return the final result)
/// DISCLAIMER: this code is intended for learning and illustrative purposes,
/// we make no claims to the accuracy or resolution of the resulting clock offsets
/// this should not be used in production codes as is.
/// --------------------------------------------------------------------------
use lamellar::{ActiveMessaging};
use std::time::SystemTime;

fn get_time_as_nsec() -> i128 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_nanos() as i128,
        Err(_) => 0,
    }
}

#[lamellar::AmData( Clone, Debug)]
struct SyncAM {}
#[lamellar::AmData( Clone, Debug)]
struct RespAM {
    time: i128,
}
#[lamellar::AmData( Clone, Debug)]
struct FollowUpAM {}

#[lamellar::am(return_am = "RespAM -> i128")]
impl LamellarAM for SyncAM {
    fn exec(&self) -> RespAM {
        let t = get_time_as_nsec();
        RespAM { time: t }
    }
}

#[lamellar::am]
impl LamellarAM for RespAM {
    fn exec(&self) -> i128 {
        let t2 = get_time_as_nsec();
        let t1 = self.time;
        let t3 = get_time_as_nsec();
        let t4 = lamellar::world
            .exec_am_pe(0, FollowUpAM {})
            .into_future()
            .await
            .expect("error returnin from am");
        let adj = -((t2 - t1) - (t4 - t3)) / 2;
        adj
    }
}

#[lamellar::am]
impl LamellarAM for FollowUpAM {
    fn exec(&self) -> i128 {
        get_time_as_nsec()
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let _my_pe = world.my_pe();
    let _num_pes = world.num_pes();
    world.barrier();
    let mut reqs = Vec::new();
    for _i in 0..100 {
        reqs.push(world.exec_am_pe(0, SyncAM {}));
    }
    world.wait_all();
    world.barrier();

    let mut sum = 0 as i128;
    for i in 0..10 {
        let res = reqs[i].get().unwrap();
        sum += res;
    }
    println!("[{:?}] adj: {:?}", _my_pe, sum / 10 as i128);
}
