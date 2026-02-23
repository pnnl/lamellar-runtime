// use lamellar::ActiveMessaging; // needed for exec_am_all
// use lamellar_main::lamellar_test;
// use tracing_subscriber::{EnvFilter, FmtSubscriber};
// use tracing::{trace, info}; // Import macros you use

// //----------------- Hello World Active message -----------------//
// #[lamellar::AmData(Debug, Clone)]
// struct HelloWorld {
//     originial_pe: usize,
// }

// #[lamellar::am]
// impl LamellarAM for HelloWorld {
//     async fn exec(self) {
//         println!(
//             "Hello World  on PE {:?} of {:?} using thread {:?}, received from PE {:?}",
//             lamellar::current_pe,
//             lamellar::num_pes,
//             std::thread::current().id(),
//             self.originial_pe,
//         );
//     }
// }

// #[lamellar_test]
// fn hello_world() {
//     let world = lamellar::LamellarWorldBuilder::new().build();
//     let my_pe = world.my_pe();
//     world.barrier();

//     //Send a Hello World Active Message to all pes
//     let request = world
//         .exec_am_all(HelloWorld {
//             originial_pe: my_pe,
//         })
//         .spawn();

//     //wait for the request to complete
//     request.block();
// } //when world drops there is an implicit world.barrier() that occurs