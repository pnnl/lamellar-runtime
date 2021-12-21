use lamellar::{ActiveMessaging, Darc, GlobalRwDarc, LocalRwDarc, StridedArch};
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
    fn exec(self) {
        println!("in darc am!!");
        self.darc.fetch_add(1, Ordering::SeqCst); //this only updates atomic on the executing pe
        println!("lrw: {:?} ", self.lrw_darc.read());
        println!("global w: {:?}", self.global_darc.async_write().await);
        println!("global r: {:?}", self.global_darc.async_read().await);
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

    // println!("here 0");

    let even_team = world.create_team_from_arch(StridedArch::new(
        0,                                      // start pe
        2,                                      // stride
        (num_pes as f64 / 2.0).ceil() as usize, //num pes in team
    ));

    // println!("here 1");

    let global_darc = GlobalRwDarc::new(world.team(), 0).unwrap();
    // println!("here 2");
    let read_lock = global_darc.read();
    println!("I have the read lock!!!! {:?}", my_pe);
    drop(read_lock);
    let write_lock = global_darc.write();
    println!("I have the write lock!!!! {:?}", my_pe);
    std::thread::sleep(std::time::Duration::from_secs(2));
    drop(write_lock);
    // println!("here3");
    let local_darc = LocalRwDarc::new(world.team(), 10).unwrap();
    println!("created new local rw");
    // local_darc.print();

    let wrapped = WrappedWrappedWrappedDarc {
        wrapped: WrappedWrappedDarc {
            wrapped: WrappedDarc {
                wrapped: Darc::new(world.team(), 3).unwrap(),
            },
        },
    };
    // println!("here 4");
    let darc1 = Darc::new(world.team(), 10).unwrap();
    // println!("here 5");
    let darc2 = Darc::new(world.team(), 20).unwrap();
    // println!("here 6");
    if let Some(team) = even_team {
        let team_darc = Darc::new(team.clone(), AtomicUsize::new(10));
        println!("created team darc");
        if let Ok(team_darc) = team_darc {
            let test = team_darc.clone();
            test.fetch_add(1, Ordering::Relaxed);
            let darc_am = DarcAm {
                darc: team_darc,
                lrw_darc: local_darc.clone(),
                global_darc: global_darc.clone(),
                wrapped: wrapped.clone(),
                wrapped_tuple: (wrapped.clone(), wrapped.clone()),
                darc_tuple: (darc1.clone(), darc2.clone()),
                my_arc: Darc::new(team.clone(), Arc::new(0)).unwrap(),
            };
            // println!("here 7");
            team.exec_am_pe(0, darc_am.clone());
            team.exec_am_all(darc_am);
            // println!("here 8");
        } else {
            println!("here");
            *(*local_darc.write()) += 1;
        }
    }
    // println!("here 9");

    drop(darc1);
    drop(darc2);
    drop(wrapped);
    println!("changing darc type");
    let ro_darc = global_darc.into_localrw().into_darc(); // we can call into_darc directly on global_Darc, but string the operations for testing purposes
    println!("read only darc");
    ro_darc.print();
}
