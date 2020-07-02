use lamellar::{ActiveMessaging,LamellarAM};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct DataAM0 {
    pe: usize,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct DataAM1 {
    pe: usize,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct AutoReturnAM {
    pe: usize,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct ReturnAM {
    pe: usize,
}

#[lamellar::am(return_am = "AutoReturnAM")]
impl LamellarAM for DataAM0 {
    fn exec() -> AutoReturnAM {
        println!("Hello on {:?} from {:?}",lamellar::current_pe,self.pe);
        AutoReturnAM{
            pe: lamellar::current_pe as usize,
        }
    }
}

#[lamellar::am]
impl LamellarAM for DataAM1 {
    fn exec() -> ReturnAM {
        println!("Hello on {:?} from {:?}",lamellar::current_pe,self.pe);
        ReturnAM{
            pe: lamellar::current_pe as usize,
        }
    }
}

#[lamellar::am]
impl LamellarAM for AutoReturnAM {
    fn exec(){
        println!("Im automatically executed on {:?} from {:?}",lamellar::current_pe,self.pe);
    }
}

#[lamellar::am]
impl LamellarAM for ReturnAM {
    fn exec(){
        println!("Im manually executed on {:?} from {:?}",lamellar::current_pe,self.pe);
    }
}



fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("my_pe {:?} num_pes {:?}", my_pe, num_pes);

    world.barrier();

    let req = world.exec_am_all(
        DataAM0{
            pe: my_pe
        }
    );
    world.wait_all();
    world.barrier();
    let result = req.get();
    println!("I return none! {:?}", result);

    let req = world.exec_am_all(
        DataAM1{
            pe: my_pe
        }
    );
    world.wait_all();
    world.barrier();
    let result = req.get();
    println!("I return a closure! {:?}", result);
    let c = result.unwrap();
    c.exec();
    world.barrier();
}
