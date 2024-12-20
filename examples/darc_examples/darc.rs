use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[lamellar::AmData(Clone)]
struct DarcAm {
    darc: Darc<AtomicUsize>, //each pe has a local atomicusize
    global_darc: GlobalRwDarc<usize>,
    lrw_darc: LocalRwDarc<usize>,
    wrapped: WrappedWrappedWrappedDarc,
    wrapped_tuple: (WrappedWrappedWrappedDarc, WrappedWrappedWrappedDarc),
    darc_tuple: (Darc<usize>, Darc<usize>), // not supported, but the macro catches it and forces compiler to fail
    // #[serde(serialize_with="lamellar::darc_serialize")]
    my_arc: Darc<Arc<usize>>,
}

#[lamellar::am]
impl LamellarAm for DarcAm {
    async fn exec(self) {
        println!("in darc am!!");
        self.darc.fetch_add(1, Ordering::SeqCst); //this only updates atomic on the executing pe
        println!("lrw: {:?} ", self.lrw_darc.read().await);
        println!("global w: {:?}", self.global_darc.write().await);
        println!("global r: {:?}", self.global_darc.read().await);
        let temp = *self.darc_tuple.0 + *self.darc_tuple.1;
        println!("temp: {:?}", temp);
        self.darc.print();
    }
}

#[lamellar::AmData(Clone)]
struct WrappedDarc {
    wrapped: Darc<usize>,
}

#[lamellar::AmData(Clone)]
struct WrappedWrappedDarc {
    wrapped: WrappedDarc,
}

#[lamellar::AmData(Clone)]
struct WrappedWrappedWrappedDarc {
    wrapped: WrappedWrappedDarc,
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let even_team = world.create_team_from_arch(StridedArch::new(
        0,                                      // start pe
        2,                                      // stride
        (num_pes as f64 / 2.0).ceil() as usize, //num pes in team
    ));

    let global_darc = GlobalRwDarc::new(world.team(), 0).block().unwrap();
    let read_lock = global_darc.read().block();
    println!("I have the read lock!!!! {:?}", my_pe);
    drop(read_lock);
    let write_lock = global_darc.write().block();
    println!("I have the write lock!!!! {:?}", my_pe);
    std::thread::sleep(std::time::Duration::from_secs(1));
    drop(write_lock);
    //----
    let local_darc = LocalRwDarc::new(world.team(), 10).block().unwrap();
    println!("created new local rw");
    // local_darc.print();

    let wrapped = WrappedWrappedWrappedDarc {
        wrapped: WrappedWrappedDarc {
            wrapped: WrappedDarc {
                wrapped: Darc::new(world.team(), 3).block().unwrap(),
            },
        },
    };
    let darc1 = Darc::new(world.team(), 10).block().unwrap();
    let darc2 = Darc::new(world.team(), 20).block().unwrap();
    if let Some(team) = even_team {
        let team_darc = Darc::new(team.clone(), AtomicUsize::new(10)).block();
        let mut tg = typed_am_group!(DarcAm, team.clone());
        println!("{:?} created team darc", std::thread::current().id());
        if let Ok(team_darc) = team_darc {
            let test = team_darc.clone();
            test.fetch_add(1, Ordering::Relaxed);
            println!("after team darc fetch add");
            let darc_am = DarcAm {
                darc: team_darc,
                lrw_darc: local_darc.clone(),
                global_darc: global_darc.clone(),
                wrapped: wrapped.clone(),
                wrapped_tuple: (wrapped.clone(), wrapped.clone()),
                darc_tuple: (darc1.clone(), darc2.clone()),
                my_arc: Darc::new(team.clone(), Arc::new(0)).block().unwrap(),
            };
            let _ = team.exec_am_pe(0, darc_am.clone()).spawn();
            let _ = team.exec_am_all(darc_am.clone()).spawn();
            tg.add_am_pe(0, darc_am.clone());
            tg.add_am_all(darc_am);
            team.block_on(tg.exec());
        } else {
            *local_darc.write().block() += 1;
        }
    }
    // --------

    // drop(darc1);
    // drop(darc2);
    // drop(wrapped);
    println!("changing darc type");
    let ro_darc = global_darc.into_localrw().block().into_darc().block(); // we can call into_darc directly on global_Darc, but string the operations for testing purposes
    println!("read only darc");
    ro_darc.print();
    println!("done");
}
