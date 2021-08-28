// use rand::Rng;
use rand::distributions::{Distribution, Uniform};
use std::time::Instant;

use lamellar::{ActiveMessaging,LamellarAM};
// use lamellar::{Backend, SchedulerType};

//----------------- Active message returning nothing-----------------//

#[derive(serde::Serialize, serde::Deserialize, Debug,Clone)]
struct AmEmpty {
}

#[lamellar::am]
impl LamellarAM for AmEmpty {
    fn exec(self) {
        // println!("in empty");
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug,Clone)]
struct AmEmptyReturnAmEmpty {
}

#[lamellar::am(return_am = "AmEmpty")]
impl LamellarAM for AmEmptyReturnAmEmpty {
    fn exec(self) ->AmEmpty {
        // println!("in return empty");
        AmEmpty{}
    }
}


#[derive(serde::Serialize, serde::Deserialize, Debug,Clone)]
struct AmNoReturn {
    my_pe: usize,
    index: usize,
    data: Vec<usize>,
}

#[lamellar::am]
impl LamellarAM for AmNoReturn {
    fn exec(self) {
        // println!("\t{:?} {:?} leaving", self.index,self.data.len());
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug,Clone)]
struct AmReturnVec {
    my_pe: usize,
    vec_size: usize,
    data: Vec<usize>,
}

#[lamellar::am]
impl LamellarAM for AmReturnVec {
    fn exec(self) -> Vec<usize>{
        // println!("\t{:?} {:?} leaving", self.vec_size,self.data.len());
        vec![0;self.vec_size]
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug,Clone)]
struct InitialAMVec {
    val1: usize,
    val2: String,
    vec: Vec<usize>,
}

#[lamellar::am(return_am = "ReturnVecAM -> Vec<usize>")] //we specify as a proc_macro argument the type of AM we are returning
impl LamellarAM for InitialAMVec {
    fn exec(&self) -> ReturnVecAM {
        let current_hostname = hostname::get().unwrap().to_string_lossy().to_string();
        // println!("{:?}",current_hostname);
        ReturnVecAM {
            val1: self.val1,
            val2: current_hostname,
            vec: vec![1;self.val1],
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug,Clone)]
struct ReturnVecAM {
    val1: usize,
    val2: String,
    vec: Vec<usize>,
}

#[lamellar::am]
impl LamellarAM for ReturnVecAM {
    fn exec(&self) -> Vec<usize> {
        // println!("return vec");
        self.vec.clone()
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new()
        //.with_lamellae(Default::default()) //if enable-rofi feature is active default is rofi, otherwise local
        //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend to rofi, with the default provider
        //.with_lamellae( Backend::RofiShm ) //explicity set the lamellae backend to rofi, specifying the shm provider
        //.with_lamellae( Backend::RofiVerbs ) //explicity set the lamellae backend to rofi, specifying the verbs provider
        //.with_lamellae( Backend::Local )
        //.with_scheduler(SchedulerType::WorkStealing) //currently the only type of thread scheduler
        .build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    let mut rng = rand::thread_rng();
    let pe_rng = Uniform::from(0..num_pes+1);
    let am_rng = Uniform::from(0..12);
    let buf_rng = Uniform::from(10000..100000);
    world.barrier();
    println!("after first barrier");
    // if my_pe == 0 {
        let s = Instant::now();
        for i in 0..10000{
            let pe = pe_rng.sample(&mut rng);
            let len1 = buf_rng.sample(&mut rng);
            let len2 = buf_rng.sample(&mut rng);
            if pe == num_pes{
                match am_rng.sample(&mut rng){
                    0 => {world.exec_am_all(AmEmpty{});},//batch msg ,batch unit return
                    1 => {world.exec_am_all(AmEmptyReturnAmEmpty{});}, //batch msg, batch return am
                    2 => {world.exec_am_all( AmNoReturn { my_pe: my_pe, index: i , data: vec![i;1]});}, //batch msg ,batch unit return
                    3 => {world.exec_am_all( AmNoReturn { my_pe: my_pe, index: i , data: vec![i;len1]});},//direct msg , batch unit return
                    4 => {world.exec_am_all( AmReturnVec { my_pe: my_pe, vec_size: 1 , data: vec![i;1]});}, //batch message, batch return
                    5 => {world.exec_am_all( AmReturnVec { my_pe: my_pe, vec_size: 1 , data: vec![i;len1]});}, //direct msg, batch return
                    6 => {world.exec_am_all( AmReturnVec { my_pe: my_pe, vec_size: 100000 , data: vec![i;1]});}, //batch message, direct return
                    7 => {world.exec_am_all( AmReturnVec { my_pe: my_pe, vec_size: 100000 , data: vec![i;len1]});}, //direct msg, direct return
                    8 => {world.exec_am_all( InitialAMVec { val1: 1, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;1]});}, //batch msg ,batch return
                    9 => {world.exec_am_all( InitialAMVec { val1: 1, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;len1]});},//direct msg , batch return
                    10 => {world.exec_am_all( InitialAMVec { val1: 100000, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;1]});}, //batch message, direct return
                    _ => {world.exec_am_all( InitialAMVec { val1: 100000, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;len1]});}, //direct msg, direct return
                }
            }
            else {
                match am_rng.sample(&mut rng){
                    0 => {world.exec_am_pe( pe, AmEmpty{});},//batch msg ,batch unit return
                    1 => {world.exec_am_pe( pe, AmEmptyReturnAmEmpty{});}, //batch msg, batch return am
                    2 => {world.exec_am_pe( pe, AmNoReturn { my_pe: my_pe, index: i , data: vec![i;1]});}, //batch msg ,batch unit return
                    3 => {world.exec_am_pe( pe, AmNoReturn { my_pe: my_pe, index: i , data: vec![i;len1]});},//direct msg , batch unit return
                    4 => {world.exec_am_pe( pe, AmReturnVec { my_pe: my_pe, vec_size: 1 , data: vec![i;1]});}, //batch message, batch return
                    5 => {world.exec_am_pe( pe, AmReturnVec { my_pe: my_pe, vec_size: 1 , data: vec![i;len1]});}, //direct msg, batch return
                    6 => {world.exec_am_pe( pe, AmReturnVec { my_pe: my_pe, vec_size: len2 , data: vec![i;1]});}, //batch message, direct return
                    7 => {world.exec_am_pe( pe, AmReturnVec { my_pe: my_pe, vec_size: len2 , data: vec![i;len1]});}, //direct msg, direct return
                    8 => {world.exec_am_pe( pe, InitialAMVec { val1: 1, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;1]});}, //batch msg ,batch return
                    9 => {world.exec_am_pe( pe, InitialAMVec { val1: 1, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;len1]});},//direct msg , batch return
                    10 => {world.exec_am_pe( pe, InitialAMVec { val1: len2, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;1]});}, //batch message, direct return
                    _ => {world.exec_am_pe( pe, InitialAMVec { val1: len2, val2: hostname::get().unwrap().to_string_lossy().to_string(), vec: vec![i;len1]});}, //direct msg, direct return
                }
            }
        }
        println!("issue time: {:?}",s.elapsed().as_secs_f64());
        world.wait_all();
        println!("local finished time: {:?}",s.elapsed().as_secs_f64());
        world.barrier();
		if my_pe == 0 {
        	println!("global finished time: {:?}",s.elapsed().as_secs_f64());
		}
    // }


    world.barrier();
    
}

