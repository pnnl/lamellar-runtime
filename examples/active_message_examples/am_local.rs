/// ------------Lamellar Example: AM Local-------------------------
/// This example highlights how to create a Local Lamellar Active message
/// The key difference between a local and remote active message is that
/// the active message struct does not need to be serialize/deserialize
/// (although currently outputs still need to be)
/// --------------------------------------------------------------------
// use lamellar::ActiveMessaging;
use lamellar::active_messaging::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
// use lamellar::{Backend, SchedulerType};

//----------------- Active message returning nothing-----------------//
#[lamellar::AmLocalData(Debug, Clone)]
struct AmNoReturn {
    my_id: usize,
    data: Arc<HashMap<usize, Vec<usize>>>,
    index: Arc<AtomicUsize>,
}

#[lamellar::local_am] //note the change from #[lamellar::am]
impl LamellarAM for AmNoReturn {
    async fn exec(self) {
        println!(
            "\tin AmNoReturn {:?} on pe {:?} of {:?} ({:?})",
            self.my_id,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap()
        );
        let mut i = self.index.fetch_add(1, Ordering::Relaxed);
        while i < self.data.len() {
            println!("{:?} {:?} {:?}", self.my_id, i, self.data.get(&i));
            std::thread::sleep(Duration::from_millis(1000));
            i = self.index.fetch_add(1, Ordering::Relaxed);
        }
        println!("\t{:?} leaving", self.my_id);
    }
}
//-----------------------------------------------------------------//
//----------------- Active message returning usize-----------------//
#[lamellar::AmLocalData(Debug, Clone)]
struct AmReturnUsize {
    my_id: usize,
    data: Arc<HashMap<usize, Vec<usize>>>,
    index: Arc<AtomicUsize>,
}
#[lamellar::local_am]
impl LamellarAM for AmReturnUsize {
    async fn exec(self) -> usize {
        println!(
            "\tin AmReturnUsize {:?} on pe {:?} of {:?} ({:?})",
            self.my_id,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap()
        );
        let mut i = self.index.fetch_add(1, Ordering::Relaxed);
        let mut sum = 0;
        while i < self.data.len() {
            sum += 1; //self.data.get(&1).unwrap().iter().sum::<usize>();
            std::thread::sleep(Duration::from_millis(1000));
            i = self.index.fetch_add(1, Ordering::Relaxed);
        }
        println!("\t{:?} leaving, sum{:?}", self.my_id, sum);
        sum
    }
}
//-----------------------------------------------------------------//

fn main() {
    let world = LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let _num_pes = world.num_pes();
    world.barrier();
    let mut map = HashMap::new();
    for i in 0..10 {
        for j in i..(10) {
            map.entry(i).or_insert(vec![]).push(j);
        }
    }
    // let am = AmNoReturn { my_pe: my_pe, data: Arc::new(map), index: Arc::new(AtomicUsize::new(0)) };
    let map = Arc::new(map);
    let index = Arc::new(AtomicUsize::new(0));
    if my_pe == 0 {
        println!("---------------------------------------------------------------");
        println!("Testing local am no return");
        for i in 0..map.len() {
            let _ = world.exec_am_local(AmNoReturn {
                my_id: i,
                data: map.clone(),
                index: index.clone(),
            });
        }
        world.wait_all();
        println!("-----------------------------------");
        println!("---------------------------------------------------------------");
        println!("Testing local am no return");
        for i in 0..map.len() {
            let _ = world.exec_am_local(AmReturnUsize {
                my_id: i,
                data: map.clone(),
                index: index.clone(),
            });
        }
        world.wait_all();
        println!("-----------------------------------");
        //     println!("---------------------------------------------------------------");
        //     println!("Testing local am no return");
        //     let res = world.exec_am_pe(my_pe, am.clone()).blocking_wait();
        //     assert_eq!(res, None);
        //     println!("no return result: {:?}", res);
        //     println!("-----------------------------------");
        //     println!("Testing remote am no return");
        //     let res = world.exec_am_pe(num_pes - 1, am.clone()).blocking_wait();
        //     assert_eq!(res, None);
        //     println!("no return result: {:?}", res);
        //     println!("-----------------------------------");
        //     println!("Testing all am no return");
        //     println!("[{:?}] exec on all", my_pe);
        //     let res = world.exec_am_all(am.clone()).blocking_wait();
        //     assert!(res.iter().all(|x| x.is_none()));
        //     println!("no return result: {:?}", res);
        //     println!("---------------------------------------------------------------");
    }

    // println!("---------------------------------------------------------------");
    // println!("Testing ring pattern am no return");
    // let res = world.exec_am_pe((my_pe + 1) % num_pes, am.clone()).blocking_wait();
    // assert_eq!(res, None);
    // println!("no return result: {:?}", res);
    // println!("-----------------------------------");
}
