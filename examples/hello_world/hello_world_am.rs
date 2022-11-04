/// ------------Lamellar Example: Hello World-------------------------
/// This example highlights how to create a Lamellar Active message
/// the active message consists of a single input value (the pe id of the originating pe)
/// when executed it will print Hello World, the PE and thread it executed on, and the orginating pe
/// --------------------------------------------------------------------
use lamellar::ActiveMessaging; // needed for exec_am_all

//----------------- Hello World Active message -----------------//
#[lamellar::AmData(Debug, Clone)]
struct HelloWorld {
    originial_pe: usize,
}

#[lamellar::am]
impl LamellarAM for HelloWorld {
    async fn exec(self) {
        println!(
            "Hello World  on PE {:?} of {:?} using thread {:?}, received from PE {:?}",
            lamellar::current_pe,
            lamellar::num_pes,
            std::thread::current().id(),
            self.originial_pe,
        );
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    world.barrier();

    //Send a Hello World Active Message to all pes
    let request = world.exec_am_all(HelloWorld {
        originial_pe: my_pe,
    });

    //wait for the request to complete
    world.block_on(request);
} //when world drops there is an implicit world.barrier() that occurs
