/// ------------Lamellar Example: Team based AM -------------------------
/// this example highlights creating and using LamellarTeams
/// to launch and execute active messages.
///----------------------------------------------------------------
use lamellar::{ActiveMessaging, BlockedArch, LamellarTeam, LamellarWorld, StridedArch};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[lamellar::AmData(Clone)]
struct TeamAM {
    secs: u64,
    orig_pe: usize,
}

#[lamellar::am]
impl LamellarAM for TeamAM {
    fn exec() {
        // let team = lamellar::team;
        // println!("current_pe: {:?}, orig_pe {:?}, team.world_pe_id(): {:?}, team.team_pe_id(): {:?} team members: {:?}",
        // lamellar::current_pe ,self.orig_pe, team.world_pe_id(), team.team_pe_id(), team.get_pes());
        async_std::task::sleep(Duration::from_secs(self.secs)).await;
    }
}

fn test_team(world: &LamellarWorld, team: Option<Arc<LamellarTeam>>, label: &str) {
    let my_pe = world.my_pe();
    if my_pe == 0 {
        println!("------------------- testing {} -------------------", label);
    }
    world.barrier();
    let elapsed = if let Some(team) = team {
        let secs = if let Ok(id) = team.team_pe_id() {
            if id == 0 {
                team.print_arch();
            }
            2
        } else {
            1
        };
        let timer = Instant::now();
        team.exec_am_all(TeamAM {
            secs: secs,
            orig_pe: my_pe,
        }); //everynode that has a handle can launch on a given team;
        team.wait_all(); //wait until all requests return
        team.barrier(); // barriers only apply to team members, its a no op for non team members
        timer.elapsed().as_secs_f64()
    } else {
        0.0
    };
    world.barrier();
    //time will be approx 2 for team members, 1 for non members
    for i in 0..world.num_pes() {
        if i == my_pe {
            println!("[{:?}] elapsed time: {:?}", my_pe, elapsed);
        }
        world.barrier();
    }
    world.barrier();
    if my_pe == 0 {
        println!("--------------------------------------------------------");
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();

    world.barrier();
    if my_pe == 0 {
        println!("-------------testing world-------------------");
    }
    world.barrier();
    let timer = Instant::now();
    world.exec_am_all(TeamAM {
        secs: 1,
        orig_pe: my_pe,
    });
    world.wait_all();
    world.barrier();
    let elapsed = timer.elapsed().as_secs_f64();
    //should all be roughly 1 equal
    for i in 0..num_pes {
        if i == my_pe {
            println!("[{:?}] elapsed time: {:?}", my_pe, elapsed);
        }
        world.barrier();
    }
    if my_pe == 0 {
        println!("---------------------------------------------");
    }
    world.barrier();

    //create a team consisting of pes 0..num_pes/2
    let first_half_team = world.create_team_from_arch(BlockedArch::new(
        0,                                      //start pe
        (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
    ));

    test_team(&world, first_half_team.clone(), "first half team");

    let even_team = world.create_team_from_arch(StridedArch::new(
        0,                                      // start pe
        2,                                      // stride
        (num_pes as f64 / 2.0).ceil() as usize, //num pes in team
    ));

    test_team(&world, even_team, "even team");
    // we can also create sub teams
    if num_pes > 3 {
        if let Some(team) = first_half_team {
            println!("going to create sub team");
            let second_half_sub_team = LamellarTeam::create_subteam_from_arch(
                team.clone(),
                BlockedArch::new(
                    (team.num_pes() as f64 / 2.0).ceil() as usize, //start pe
                    (team.num_pes() as f64 / 2.0).floor() as usize, //num_pes in team
                ),
            );

            test_team(&world, second_half_sub_team.clone(), "second half sub team");
        }
    }
}
