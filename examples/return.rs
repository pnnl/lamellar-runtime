use lamellar::{ActiveMessaging,LamellarAM};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct DataAM {
    pe: usize,
}

#[lamellar::am]
impl LamellarAM for DataAM {
    fn exec() -> (usize,usize) {
        (self.pe,lamellar::current_pe as usize)
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);
    world.barrier();
    println!("exec_on_pe returns");
    let mut reqs = vec![];
    for pe in 0..num_pes {
        reqs.push(world.exec_am_pe(
            pe,
            DataAM{
                pe: my_pe
            },
        ));
    }
    for req in reqs {
        let res = req.get();
        println!("{:?}", res);
    }
    world.barrier();
    println!("--------------------------------------------");

    println!("exec_all returns");
    let req = world.exec_am_all(
        DataAM{
            pe: my_pe
        },
    );
    let res = req.get_all();
    println!("{:?}", res);
    world.barrier();
    println!("--------------------------------------------");

    world.barrier();
}
