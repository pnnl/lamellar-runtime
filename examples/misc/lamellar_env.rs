/// This example simply showcases the LamellarEnv Trait
use lamellar::array::prelude::*;
use lamellar::darc::prelude::*;
use lamellar::lamellar_env::LamellarEnv;

fn print_env<T: LamellarEnv>(env: &T) {
    println!("my_pe: {}", env.my_pe());
    println!("num_pes: {}", env.num_pes());
    println!("num_threads_per_pe: {}", env.num_threads_per_pe());
    println!("world: {:?}", env.world());
    println!("team: {:?}", env.team());
    println!();
}

fn main() {
    let world = LamellarWorldBuilder::new().build();
    let darc = Darc::new(&world, 0).block().unwrap();
    let lrw_darc = LocalRwDarc::new(&world, 0).block().unwrap();
    let grw_darc = GlobalRwDarc::new(&world, 0).block().unwrap();
    let array = UnsafeArray::<u8>::new(world.clone(), 10, Distribution::Block).block();
    let team = world
        .create_team_from_arch(StridedArch::new(0, 2, world.num_pes() / 2))
        .unwrap();
    println!("environment from world");
    print_env(&world);
    println!("environment from darc");
    print_env(&darc);
    println!("environment from lrw_darc");
    print_env(&lrw_darc);
    println!("environment from grw_darc");
    print_env(&grw_darc);
    println!("environment from UnsafeArray");
    print_env(&array);
    let array = array.into_atomic();
    println!("environment from AtomicArray");
    print_env(&array);
    let array = array.into_local_lock();
    println!("environment from LocalOnlyArray");
    print_env(&array);
    let array = array.into_global_lock();
    println!("environment from GlobalLockArray");
    print_env(&array);
    if world.my_pe() % 2 == 0 {
        println!("environment from team");
        print_env(&team);
    }
}
