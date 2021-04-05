use lamellar::{ActiveMessaging, LamellarAM};

//----------------- Recursive Active Message -----------------//
// in this example we launch new active messages from within
// a currently executing active message. Specifically we visit
// each PE in the allocation returning a list of their
// host names in reverse order of how we visited them.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct RecursiveAM {
    next: usize,
    orig: usize,
}

#[lamellar::am]
impl LamellarAM for RecursiveAM {
    fn exec(&self) -> Vec<String> {
        println!(
            "\tin RecursiveAM {:?} on pe {:?} of {:?} ({:?})",
            self,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap().into_string().unwrap()
        );
        let next_pe = (self.next + 1) % lamellar::team.num_pes() as usize;
        if next_pe == self.orig {
            //this is the terminating condition to end recursion
            let mut res = Vec::new();
            res.push(hostname::get().unwrap().into_string().unwrap()); //add my hostname as first in the list
            res
        } else {
            let next = lamellar::team.exec_am_pe(
                next_pe,
                RecursiveAM {
                    next: next_pe,
                    orig: self.orig,
                },
            );
            // let mut res = next.get().expect("error returning from am"); // this will cause deadlock
            let mut res = next.into_future().await.expect("error returning from am");
            res.push(hostname::get().unwrap().into_string().unwrap()); //append my host name to list returned from previous call
            res
        }
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    world.barrier();
    world.team.barrier();
    if my_pe == 0 {
        println!("---------------------------------------------------------------");
        println!("testing recursive am");
        let res = world
            .exec_am_pe(
                my_pe,
                RecursiveAM {
                    next: my_pe,
                    orig: my_pe,
                },
            )
            .get_all();
        println!("visit paths: {:?}", res);
        println!("---------------------------------------------------------------");
    }
    world.team.barrier();
}
