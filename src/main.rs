use lamellar::{
    ActiveMessaging,  LamellarAM, LamellarMemoryRegion, RemoteMemoryRegion, SchedulerType,StridedArch
};
use std::time::Instant;

//----------------- Active message returning nothing-----------------//
#[derive(serde::Serialize, serde::Deserialize,  Debug)]
struct AmNoReturn {
    my_pe: usize,
}

#[lamellar::am]
impl LamellarAM for AmNoReturn {
    fn exec(&self) {
        println!(
            "in AmNoReturn {:?} on pe {:?} of {:?} ({:?})",
            self,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap()
        );
    }
}
//-------------------------------------------------------------------//

//----------------- Active message returning data--------------------//
 #[derive(serde::Serialize, serde::Deserialize,  Clone, Debug)] //eventually derive lamellar::Return to automatically implement "LamellarDataReturn"
struct AmReturnUsize {
    temp: usize,
    talk: String,
}

#[lamellar::am]
impl LamellarAM for AmReturnUsize {
    fn exec(&self) -> usize {
        println!(
            "in  AmReturnUsize {:?} self {:?} on pe {:?} of {:?} ({:?})",
            self,
            self.talk,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap()
        );
        let mut ret = self.clone();
        ret.temp = lamellar::current_pe as usize;
        println!("ret: {:?}", ret);
        // self.temp
        ret.temp
    }
}
//-------------------------------------------------------------------//

//--Active message returning an active message that returns nothing--//
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)] //eventually derive lamellar::Return to automatically implement "LamellarDataReturn"
struct AmReturnAm {
    temp: usize,
    talk: String,
}

#[lamellar::am(return_am = "AmNoReturn")]
impl LamellarAM for AmReturnAm {
    fn exec(&self) -> AmNoReturn {
        println!(
            "in  AmReturnAm {:?} self {:?} on pe {:?} of {:?} ({:?})",
            self,
            self.talk,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap()
        );
        AmNoReturn {
            my_pe: lamellar::current_pe as usize,
        }
    }
}
//-------------------------------------------------------------------//

//----Active message returning an active message that returns data---//
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)] //eventually derive lamellar::Return to automatically implement "LamellarDataReturn"
struct AmReturnAmUsize {
    temp: usize,
    talk: String,
}

#[lamellar::am(return_am = "AmReturnUsize(usize)")]
impl LamellarAM for AmReturnAmUsize {
    fn exec(&self) -> AmReturnUsize {
        println!(
            "in  AmReturnAmUsize {:?} self {:?} on pe {:?} of {:?} ({:?}) ",
            self,
            self.talk,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap()
        );
        AmReturnUsize {
            temp: lamellar::current_pe as usize,
            talk: "auto_executed!!!".to_string(),
        }
    }
}
//-------------------------------------------------------------------//

//----------------- Active message returning nothing-----------------//
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct AmLMR {
    lmr: LamellarMemoryRegion<u8>,
}

#[lamellar::am]
impl LamellarAM for AmLMR {
    fn exec(&self) {
        println!(
            "in AmLMR {:?} on pe {:?} of {:?} ({:?})",
            self,
            lamellar::current_pe,
            lamellar::num_pes,
            hostname::get().unwrap()
        );
    }
}
//-------------------------------------------------------------------//



fn world_based_tests(world: &lamellar::LamellarWorld){
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("starting world tests");
    
    let lmr: LamellarMemoryRegion<u8> = world.alloc_mem_region(100);
    println!("lmr: {:?} on {:?}", lmr, my_pe);
    if my_pe == 0 {
        println!("Testing local am no return");
        let res = world
            .exec_am_pe(0, AmNoReturn { my_pe: my_pe })
            .get();
        println!("no return result: {:?}", res);
        println!("Testing remote am no return");
        let res = world
            .exec_am_pe(num_pes-1, AmNoReturn { my_pe: my_pe })
            .get();
        println!("no return result: {:?}", res);
        println!("Testing all am no return");
        let res = world.exec_am_all(AmNoReturn { my_pe: my_pe }).get_all();
        println!("no return result: {:?}", res);
        println!("---------------------------------------------------------------");
        println!("Testing local am usize return");
        let res = world
            .exec_am_pe(
                0,
                AmReturnUsize {
                    temp: my_pe,
                    talk: "hello".to_string(),
                },
            )
            .get();
        println!("usize return result: {:?}", res);
        println!("Testing remote am usize return");
        let res = world
            .exec_am_pe(
                num_pes-1,
                AmReturnUsize {
                    temp: my_pe,
                    talk: "hello".to_string(),
                },
            )
            .get();
        println!("usize return result: {:?}", res);
        println!("Testing all am usize return");
        let res = world
            .exec_am_all(AmReturnUsize {
                temp: my_pe,
                talk: "hello".to_string(),
            })
            .get_all();
        println!("usize return result: {:?}", res);
        println!("---------------------------------------------------------------");
        println!("Testing local am returning an am");
        let res = world
            .exec_am_pe(
                0,
                AmReturnAm {
                    temp: my_pe,
                    talk: "hello".to_string(),
                },
            )
            .get();
        println!("am return result: {:?}", res);
        println!("Testing remote am returning an am");
        let res = world
            .exec_am_pe(
                num_pes-1,
                AmReturnAm {
                    temp: my_pe,
                    talk: "hello".to_string(),
                },
            )
            .get();
        println!("am return result: {:?}", res);
        println!("Testing all am returning an am");
        let res = world
            .exec_am_all(AmReturnAm {
                temp: my_pe,
                talk: "hello".to_string(),
            })
            .get_all();
        println!("am return result: {:?}", res);
        println!("---------------------------------------------------------------");
        println!("Testing local am returning an am returning a usize");
        let res = world
            .exec_am_pe(
                0,
                AmReturnAmUsize {
                    temp: my_pe,
                    talk: "hello".to_string(),
                },
            )
            .get();
        println!("am return result: {:?}", res);
        println!("Testing remote am returning an am returning a usize");
        let res = world
            .exec_am_pe(
                num_pes-1,
                AmReturnAmUsize {
                    temp: my_pe,
                    talk: "hello".to_string(),
                },
            )
            .get();
        println!("am return result: {:?}", res);
        println!("Testing all am returning an am returning a usize");
        let res = world
            .exec_am_all(AmReturnAmUsize {
                temp: my_pe,
                talk: "hello".to_string(),
            })
            .get_all();
        println!("am return result: {:?}", res);
        println!("---------------------------------------------------------------");

        let res = world.exec_am_all(AmLMR { lmr: lmr.clone() }).get_all();
        println!("am return result: {:?}",res);
        println!("---------------------------------------------------------------");
        
    }
    world.barrier();
    world.free_memory_region(lmr);
    println!("leaving world tests");
   
}

fn team_based_tests(world: &mut lamellar::LamellarWorld){
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    println!("starting team tests");
    let team = world.create_team_from_arch(StridedArch::new(
       0,num_pes-1,2,num_pes
    )); // should be a team consisting of the even nodes
    println!("team pes: {:?}",team.get_pes());
    println!("Testing team local am no return");
    let res = team
        .exec_am_pe(0, AmNoReturn { my_pe: my_pe })
        .get();
    println!("team no return result: {:?}", res);
    team.barrier();
    println!("---------------------------------------------------------------");
    println!("Testing team remote am no return");
    let res = team
        .exec_am_pe(team.num_pes()-1, AmNoReturn { my_pe: my_pe })
        .get();
    println!("team no return result: {:?}", res);
    team.barrier();
    println!("---------------------------------------------------------------");
    println!("Testing team all am no return");
    let res = team.exec_am_all(AmNoReturn { my_pe: my_pe }).get_all();
    println!("team no return result: {:?}", res);
    println!("---------------------------------------------------------------");
    team.barrier();
    println!("leaving  team tests");
}



fn main() { 
    let mut world = lamellar::LamellarWorldBuilder::new()
        .with_lamellae( Default::default() ) //if enable-rofi feature is active defaul is rofi, otherwise local
        //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend
        //.with_lamellae( Backend::Local )
        .with_scheduler(SchedulerType::WorkStealing)
        .build();

    let _my_pe = world.my_pe();
    let _num_pes = world.num_pes();
    world.barrier();
    let s = Instant::now();
    world.barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);
    println!("---------------------------------------------------------------");
    world.barrier();
    let s = Instant::now();
    world.team_barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Team Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);
    println!("---------------------------------------------------------------");
    world.team_barrier();
    world.team_barrier();
    world.team_barrier();
    world_based_tests(&world);

    team_based_tests(&mut world);
    world.barrier();
    println!("tests done!!!");
}
