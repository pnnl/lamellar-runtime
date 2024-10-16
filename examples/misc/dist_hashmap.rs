use lamellar::active_messaging::prelude::*;
use lamellar::darc::prelude::*;
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

#[derive(Clone)]
struct DistHashMap {
    num_pes: usize,
    team: Arc<LamellarTeam>,
    data: LocalRwDarc<HashMap<i32, i32>>, //unforunately we can't use generics here due to constraints imposed by ActiveMessages
}

impl DistHashMap {
    fn new(world: &LamellarWorld, num_pes: usize) -> Self {
        let team = world.team();
        DistHashMap {
            num_pes,
            team: team.clone(),
            data: LocalRwDarc::new(team, HashMap::new()).unwrap(),
        }
    }

    fn get_key_pe(&self, k: i32) -> usize {
        k as usize % self.num_pes
    }
    fn add(&self, k: i32, v: i32) -> impl Future {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashMapOp {
                data: self.data.clone(),
                cmd: DistCmd::Add(k, v),
            },
        )
    }

    fn get(&self, k: i32) -> impl Future<Output = DistCmdResult> {
        let dest_pe = self.get_key_pe(k);
        self.team.exec_am_pe(
            dest_pe,
            DistHashMapOp {
                data: self.data.clone(),
                cmd: DistCmd::Get(k),
            },
        )
    }
}

// this is one way we can implement commands for the distributed hashmap
// a maybe more efficient way to do this would be to create and individual
// active message for each command
// #[AmData(Debug, Clone)] eventually we will be able to do this... instead  derive serialize and deserialize directly with serde
#[derive(Debug, Clone, Serialize, Deserialize)]
enum DistCmd {
    Add(i32, i32),
    Get(i32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum DistCmdResult {
    Add,
    Get(i32),
}

#[AmData(Debug, Clone)]
struct DistHashMapOp {
    data: LocalRwDarc<HashMap<i32, i32>>, //unforunately we can't use generics here due to constraints imposed by ActiveMessages
    cmd: DistCmd,
}

#[am]
impl LamellarAM for DistHashMapOp {
    async fn exec(self) -> DistCmdResult {
        match self.cmd {
            DistCmd::Add(k, v) => {
                self.data.write().await.insert(k, v);
                DistCmdResult::Add
            }
            DistCmd::Get(k) => {
                let data = self.data.read().await;
                let v = data.get(&k);
                println!("{}", v.unwrap());
                DistCmdResult::Get(k)
            }
        }
    }
}

fn main() {
    let world = LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    world.barrier();
    let distributed_map = DistHashMap::new(&world, num_pes);

    for i in 0..10 {
        // we can ignore the 'unused' result here because we call 'wait_all' below, otherwise to ensure each request completed we could use 'block_on'
        world.block_on(distributed_map.add(i, i));
    }
    world.wait_all();
    world.barrier();
    let map_clone = distributed_map.clone();
    world.block_on(async move {
        for i in 0..10 {
            println!("{}: {:?}", i, map_clone.get(i).await);
        }
    });

    world.barrier();
    println!(
        "[{my_pe}] local data: {:?}",
        distributed_map.data.read().block()
    );
}
