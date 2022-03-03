///-------------------Lamellar Example: Custom Team Arch-----------------------------
/// This example shows how to implement a custom team layout using the LamellarArch trait:
/*
pub trait LamellarArch: Sync + Send +  std::fmt::Debug {
    fn num_pes(&self) -> usize;
    fn start_pe(&self) -> usize; // the first pe of a team with respect to parent pe ids (inclusive)
    fn end_pe(&self) -> usize;   // the last pe of a team with respect to parent pe ids (inclusive)
    fn parent_pe_id(&self, team_pe: &usize) -> Result<usize, IdError>; // parent id is used to calculate the original world pe to perform communication
    fn team_pe_id(&self, parent_pe: &usize) -> Result<usize, IdError>; // team id is for user convenience, ids == 0..num_team_pes-1
}
*/
/// Lamellar currently provides a Block (sequential) layout, as well as a strided layout.
/// this example shows how to implement a block+strided layout architecture.
///-------------------------------------------------------------------------------
use lamellar::{ActiveMessaging, LamellarTeam, LamellarWorld};
use lamellar::{IdError, LamellarArch};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Hash, Clone)]
struct BlockStridedArch {
    num_pes: usize,
    start_pe: usize, //this is with respect to the parent arch (inclusive)
    end_pe: usize,   //this is with respect to the parent arch (inclusive)
    stride: usize,
    block_size: usize,
}

impl BlockStridedArch {
    pub fn new(
        start_pe: usize,
        stride: usize,
        block_size: usize,
        num_team_pes: usize,
    ) -> BlockStridedArch {
        let num_blocks = num_team_pes / block_size;
        let remainder = num_team_pes % block_size;
        let end_pe = if remainder > 0 {
            start_pe + num_blocks * block_size * stride + remainder - 1
        } else {
            start_pe + (num_blocks - 1) * block_size * stride + block_size - 1
        };

        BlockStridedArch {
            num_pes: num_team_pes,
            start_pe: start_pe,
            end_pe: end_pe,
            stride: stride,
            block_size: block_size,
        }
    }
}

impl LamellarArch for BlockStridedArch {
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn start_pe(&self) -> usize {
        self.start_pe
    }
    fn end_pe(&self) -> usize {
        self.end_pe
    }
    fn parent_pe_id(&self, team_pe: &usize) -> Result<usize, IdError> {
        let block = team_pe / self.block_size;
        let remainder = team_pe % self.block_size;

        let parent_pe = self.start_pe + block * self.stride * self.block_size + remainder;
        if self.start_pe <= parent_pe && parent_pe <= self.end_pe {
            Ok(parent_pe)
        } else {
            Err(IdError {
                parent_pe: parent_pe,
                team_pe: *team_pe,
            })
        }
    }
    fn team_pe_id(&self, parent_pe: &usize) -> Result<usize, IdError> {
        let block = parent_pe / self.block_size;
        let start_block = self.start_pe / self.block_size;
        let remainder = parent_pe % self.block_size;
        if (block - start_block) % self.stride == 0
            && self.start_pe <= *parent_pe
            && *parent_pe <= self.end_pe
        {
            let team_pe = ((block - start_block) / self.stride) * self.block_size + remainder;
            if team_pe < self.num_pes {
                Ok(team_pe)
            } else {
                Err(IdError {
                    parent_pe: *parent_pe,
                    team_pe: team_pe,
                })
            }
        } else {
            Err(IdError {
                parent_pe: *parent_pe,
                team_pe: 0,
            })
        }
    }
}

#[lamellar::AmData(Clone)]
struct TeamAM {
    secs: u64,
}

#[lamellar::am]
impl LamellarAM for TeamAM {
    fn exec() {
        // let team = lamellar::team;
        // println!("current_pe: {:?}, team.global_pe_id(): {:?}, team.team_pe_id(): {:?} team members: {:?}",
        // lamellar::current_pe , team.global_pe_id(), team.team_pe_id(), team.get_pes());
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
        team.exec_am_all(TeamAM { secs }); //everynode that has a handle can launch on a given team;
        team.wait_all(); //wait until all requests return
        team.barrier(); // barriers only apply to team members, its a no op for non team members
        timer.elapsed().as_secs_f64()
    } else {
        0.0
    };
    println!("done");
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
    world.barrier();
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
    world.exec_am_all(TeamAM { secs: 1 });
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

    if num_pes > 1 {
        let arch = BlockStridedArch::new(
            1,                                      //start pe (world pe)
            2,                                      //stride
            1,                                      //block size
            (num_pes as f64 / 2.0).ceil() as usize, //num pes in team
        );
        let odd_team = world.create_team_from_arch(arch);
        test_team(&world, odd_team, "odd team");
    }

    world.barrier();

    let arch = BlockStridedArch::new(
        0,                                      //start pe (world pe)
        1,                                      //stride
        (num_pes as f64 / 2.0).ceil() as usize, //block size
        (num_pes as f64 / 2.0).ceil() as usize, //num pes in team
    );

    let first_half_team = world.create_team_from_arch(arch);
    test_team(&world, first_half_team, "first half team");

    let arch = BlockStridedArch::new(
        0,                                      //start pe (world pe)
        3,                                      //stride
        3,                                      //block size
        (num_pes as f64 / 3.0).ceil() as usize, //num pes in team
    );
    let blk_stride_team = world.create_team_from_arch(arch);
    test_team(&world, blk_stride_team, "blk stride team");
}
