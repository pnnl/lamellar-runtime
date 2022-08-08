/// ----------------------Lamellar Example: Random Team----------------------
/// an experimental example that implements a custom lamellar arch
/// which "scrambles" the pe id of the compute nodes. It uses
/// recursive ams to visit and print out a path of pes visted
/// also experiments with sub_teams
///------------------------------------------------------------------------
use lamellar::{ActiveMessaging, IdError, LamellarArch, LamellarTeam};
use rand::seq::SliceRandom;
// use rand::thread_rng;
use rand::{rngs::StdRng, SeedableRng};

//----------------- Recursive Active Message -----------------//
#[lamellar::AmData(Clone, Debug)]
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
            hostname::get().unwrap()
        );
        let next_pe = (self.next + 1) % lamellar::team.num_pes() as usize;
        println!("\tnext_pe = {:?}", next_pe);
        if next_pe == self.orig {
            let mut res = Vec::new();
            let string = format!(
                "[{:?}] {}",
                lamellar::current_pe,
                hostname::get().unwrap().into_string().unwrap()
            );
            res.push(string);
            res
        } else {
            let next = lamellar::team.exec_am_pe(
                next_pe,
                RecursiveAM {
                    next: next_pe,
                    orig: self.orig,
                },
            );
            let mut res = next.await;
            let string = format!(
                "[{:?}] {}",
                lamellar::current_pe,
                hostname::get().unwrap().into_string().unwrap()
            );
            res.push(string);
            res
        }
    }
}

#[lamellar::AmData(Clone, Debug)]
struct DataAM0 {
    global_pe: usize,
    parent_pe: Option<usize>,
    team_pe: Option<usize>,
}

#[lamellar::am]
impl LamellarAM for DataAM0 {
    fn exec() {
        println!("Hello on {:?} from {:?}", lamellar::current_pe, self);
    }
}

#[derive(Debug, Hash, Clone)]
struct RandomArch {
    pes: Vec<usize>, //PEs are with respect to the parent team (i.e. these are not global IDs)
    min_pe: usize,
    max_pe: usize,
}

impl RandomArch {
    fn new(min_pe: usize, max_pe: usize, num_pes: usize) -> RandomArch {
        let mut rng = StdRng::seed_from_u64((min_pe + max_pe + num_pes) as u64);
        let mut shuffled: Vec<usize> = (min_pe..max_pe).collect();
        shuffled.shuffle(&mut rng);
        let mut vec = vec![];
        vec.extend_from_slice(&shuffled[0..num_pes]);
        let min_pe = *vec.iter().min().unwrap();
        let max_pe = *vec.iter().max().unwrap();
        // println!("RandomArch: {:?} {:?} {:?}", vec, min_pe, max_pe);
        RandomArch {
            pes: vec,
            min_pe: min_pe,
            max_pe: max_pe,
        }
    }
    fn team_id(&self, pe: usize) -> Option<usize> {
        let mut i: usize = 0;
        for innerpe in &self.pes {
            if pe == *innerpe {
                return Some(i);
            }
            i += 1;
        }
        None
    }
}

impl LamellarArch for RandomArch {
    fn num_pes(&self) -> usize {
        self.pes.len()
    }
    fn start_pe(&self) -> usize {
        self.min_pe
    }
    fn end_pe(&self) -> usize {
        self.max_pe
    }
    fn parent_pe_id(&self, team_pe: &usize) -> Result<usize, IdError> {
        if team_pe < &self.pes.len() {
            Ok(self.pes[*team_pe])
        } else {
            Err(IdError {
                parent_pe: *team_pe,
                team_pe: *team_pe,
            })
        }
    }
    fn team_pe_id(&self, parent_pe: &usize) -> Result<usize, IdError> {
        // team id is for user convenience, ids == 0..num_pes-1
        // println!("searching for team_pe from parent_pe: {:?} -- {:?}",parent_pe,self.pes);
        let mut i: usize = 0;
        let mut res = Err(IdError {
            parent_pe: *parent_pe,
            team_pe: *parent_pe,
        });
        for pe in &self.pes {
            if pe == parent_pe {
                res = Ok(i);
                break;
            }
            i += 1;
        }
        res
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    if num_pes > 1 {
        world.barrier();
        let rand_arch = RandomArch::new(0, num_pes, num_pes);
        let random_team = world
            .create_team_from_arch(rand_arch.clone())
            .expect("unable to create team");
        if my_pe == 0 {
            random_team.print_arch();
        }
        let team_arch = RandomArch::new(0, num_pes, 2);
        let team = LamellarTeam::create_subteam_from_arch(random_team.clone(), team_arch.clone())
            .expect("unable to create subteam");
        if my_pe == 0 {
            team.print_arch();
        }
        for i in 0..2 {
            let p = rand_arch.team_id(my_pe);
            let t = if let Some(p) = p {
                team_arch.team_id(p)
            } else {
                None
            };
            let d = DataAM0 {
                global_pe: my_pe,
                parent_pe: p,
                team_pe: t,
            };
            println!("launching {:?} to pe {:?}", d, i);
            team.exec_am_pe(i, d);
        }

        let p = rand_arch.team_id(my_pe);
        let t = if let Some(p) = p {
            team_arch.team_id(p)
        } else {
            None
        };
        let sub_team_path = if let Some(t) = t {
            team.exec_am_pe(t, RecursiveAM { next: t, orig: t })
        } else {
            team.exec_am_pe(0, RecursiveAM { next: 0, orig: 0 })
        };
        let world_c = world.clone();
        world.block_on(async move{
        for pe in 0..num_pes {
            if pe == my_pe {
                println!("[{:?}] sub_team_path: {:?}", pe, sub_team_path.await);
            }
            world_c.barrier();
        }});
    } else {
        println!("Random team example is intended for multi pe execution");
    }
}
