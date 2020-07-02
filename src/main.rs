use lamellar::{
    ActiveMessaging, Backend, LamellarAM, LamellarMemoryRegion, RemoteMemoryRegion, SchedulerType,
};
use std::time::Instant;

//----------------- Active message returning nothing-----------------//
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct AmNoReturn {
    my_pe: usize,
}

#[lamellar::am]
impl LamellarAM for AmNoReturn {
    fn exec(&self) {
        println!(
            "in AmNoReturn {:?} on pe {:?} of {:?}",
            self,
            lamellar::current_pe,
            lamellar::num_pes
        );
    }
}
//-------------------------------------------------------------------//

//----------------- Active message returning data--------------------//
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)] //eventually derive lamellar::Return to automatically implement "LamellarDataReturn"
struct AmReturnUsize {
    temp: usize,
    talk: String,
}

#[lamellar::am]
impl LamellarAM for AmReturnUsize {
    fn exec(&self) -> usize {
        println!(
            "in  AmReturnUsize {:?} self {:?} on pe {:?} of {:?}",
            self,
            self.talk,
            lamellar::current_pe,
            lamellar::num_pes
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
            "in  AmReturnAm {:?} self {:?} on pe {:?} of {:?}",
            self,
            self.talk,
            lamellar::current_pe,
            lamellar::num_pes
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
            "in  AmReturnAmUsize {:?} self {:?} on pe {:?} of {:?} ",
            self,
            self.talk,
            lamellar::current_pe,
            lamellar::num_pes
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
            "in AmLMR {:?} on pe {:?} of {:?}",
            self,
            lamellar::current_pe,
            lamellar::num_pes
        );
    }
}
//-------------------------------------------------------------------//

fn main() {
    let world = lamellar::LamellarWorldBuilder::new()
        .with_lamellae(Backend::Rofi)
        .with_scheduler(SchedulerType::WorkStealing)
        .build();

    let my_pe = world.my_pe();
    let _num_pes = world.num_pes();
    world.barrier();
    let s = Instant::now();
    world.barrier();
    let b = s.elapsed().as_secs_f64();
    println!("Barrier latency: {:?}s {:?}us", b, b * 1_000_000 as f64);
    println!("---------------------------------------------------------------");
    let lmr: LamellarMemoryRegion<u8> = world.alloc_mem_region(100);
    println!("lmr: {:?} on {:?}", lmr, my_pe);
    if my_pe == 0 {
        println!("Testing local am no return");
        let res = world
            .exec_am_pe(0, AmNoReturn { my_pe: my_pe })
            .am_get_new();
        println!("no return result: {:?}", res);
        println!("Testing remote am no return");
        let res = world
            .exec_am_pe(1, AmNoReturn { my_pe: my_pe })
            .am_get_new();
        println!("no return result: {:?}", res);
        println!("Testing all am no return");
        let res = world.exec_am_all(AmNoReturn { my_pe: my_pe }).am_get_all();
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
            .am_get_new();
        println!("usize return result: {:?}", res);
        println!("Testing remote am usize return");
        let res = world
            .exec_am_pe(
                1,
                AmReturnUsize {
                    temp: my_pe,
                    talk: "hello".to_string(),
                },
            )
            .am_get_new();
        println!("usize return result: {:?}", res);
        println!("Testing all am usize return");
        let res = world
            .exec_am_all(AmReturnUsize {
                temp: my_pe,
                talk: "hello".to_string(),
            })
            .am_get_all();
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
            .am_get_new();
        println!("am return result: {:?}", res);
        println!("Testing remote am returning an am");
        let res = world
            .exec_am_pe(
                1,
                AmReturnAm {
                    temp: my_pe,
                    talk: "hello".to_string(),
                },
            )
            .am_get_new();
        println!("am return result: {:?}", res);
        println!("Testing all am returning an am");
        let res = world
            .exec_am_all(AmReturnAm {
                temp: my_pe,
                talk: "hello".to_string(),
            })
            .am_get_all();
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
            .am_get_new();
        println!("am return result: {:?}", res);
        println!("Testing remote am returning an am returning a usize");
        let res = world
            .exec_am_pe(
                1,
                AmReturnAmUsize {
                    temp: my_pe,
                    talk: "hello".to_string(),
                },
            )
            .am_get_new();
        println!("am return result: {:?}", res);
        println!("Testing all am returning an am returning a usize");
        let res = world
            .exec_am_all(AmReturnAmUsize {
                temp: my_pe,
                talk: "hello".to_string(),
            })
            .am_get_all();
        println!("am return result: {:?}", res);
        println!("---------------------------------------------------------------");

        let _res = world.exec_am_all(AmLMR { lmr: lmr }).am_get_new();
    }
    world.barrier();
}
