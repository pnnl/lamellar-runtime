use lamellar::{Darc,ActiveMessaging,LamellarAM,LamellarTeam,StridedArch};
use std::sync::atomic::{AtomicUsize,Ordering};

#[lamellar::AmData]
struct DarcAm{
    darc: Darc<AtomicUsize> //each pe has a local atomicusize
}

#[lamellar::am]
impl LamellarAm for DarcAm{
    fn exec(self){
        println!("in darc am!!");
        self.darc.fetch_add(1,Ordering::SeqCst); //this only updates atomic on the executing pe
    }
}


fn main() {
    let args: Vec<String> = std::env::args().collect();
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let even_team = world.create_team_from_arch(StridedArch::new(
        0,                                      // start pe
        2,                                      // stride
        (num_pes as f64 / 2.0).ceil() as usize, //num pes in team
    ));

    if let Some(team) = even_team{
        let test = Darc::try_new(team.clone(),AtomicUsize::new(10));
        if let Ok(test) = test  {
            // print!("test: ");
            // test.print();
            // println!("test val: {:?}",test);
            // let test2 = test.clone();
            // test2.fetch_add(1,Ordering::Relaxed);
            // // print!("test2: ");
            // // test2.print();
            // println!("test2 val: {:?} {:?}",test,test2);
            // drop(test);
            // // print!("test3: ");
            // // test2.print();
            // println!("test3 val: {:?}",test2);
            let darc_am = DarcAm{darc: test};
            // print!("test3.1: ");
            // darc_am.darc.print();
            // println!("test3.1 val: {:?}",darc_am.darc );
            team.exec_am_pe(0,darc_am.clone());
            team.exec_am_all(darc_am);
            // test2.print();
            // println!("test3.1 val: {:?}",test2 );
            if my_pe==0{
                // drop(test2);
                std::thread::sleep(std::time::Duration::from_secs(1));
                // world.barrier();
            }
            else{
                // world.barrier();
                std::thread::sleep(std::time::Duration::from_secs(1));
                // print!("test4: ");
                // test2.print();
                // println!("test4 val: {:?}",test2);
                // drop(test2)
            }
        }
    }

}