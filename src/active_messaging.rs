//! Active Messages are a computing model where messages contain both data (that you want to compute something with) and metadata
//! that tells the message how to process its data when it arrives at its destination, e.g. a function pointer. The Wikipedia Page <https://en.wikipedia.org/wiki/Active_message>
//! provides a short overview.
//!
//! Lamellar is built upon asynchronous active messages, and provides users with an interface to construct their own active messages.
//!
//! This interface is exposed through multiple Rust procedural macros and APIS.
//! - [AmData]
//! - [am]
//! - [AmLocalData]
//! - [local_am]
//! - [AmGroup](crate::lamellar_task_group::AmGroup)
//! - [typed_am_group]
//!
//! Further details are provided in the documentation for each macro but at a high level to implement an active message we need to
//! define the data to be transfered in a message and then define what to do with that data when we arrive at the destination.
//!
//! The following examples will cover the following topics
//! - Construncting your first Active Message
//! - Lamellar AM DSL
//! - Lamellar AM return types
//!     - returning plain old data
//!     - returning active messages
//!     - returning active messages that reutrn data
//! - Nested Active Messages
//! - Active Message Groups
//!     - Generic Active Message Groups
//!     - 'Typed' Active Message Groups
//!         - static members
//!
//! # Examples
//! Let's implement a simple active message below:
//!
//! First lets define the data we would like to transfer
//!```
//!#[derive(Debug,Clone)]
//! struct HelloWorld {
//!    original_pe: usize, //this will contain the ID of the PE this data originated from
//! }
//!```
//! This looks like a pretty normal (if simple) struct, we next have to let the runtime know we would like this data
//! to be used in an active message, so we need to apply the [AmGroup](crate::lamellar_task_group::AmGroup) macro, this is done by replacing the `derive` macro:
//!```
//! use lamellar::active_messaging::prelude::*;
//! #[AmData(Debug,Clone)]
//! struct HelloWorld {
//!    original_pe: usize, //this will contain the ID of the PE this data originated from
//! }
//!```
//! This change allows the compiler to implement the proper traits (related to Serialization and Deserialization) that will let this data type
//! be used in an active message.
//!
//! Next we now need to define the processing that we would like to take place when a message arrives at another PE
//!
//! For this we use the [am] macro on an implementation of the [LamellarAM] trait
//!```
//! # use lamellar::active_messaging::prelude::*;
//! # #[AmData(Debug,Clone)]
//! # struct HelloWorld {
//! #    original_pe: usize, //this will contain the ID of the PE this data originated from
//! # }
//! #[lamellar::am]
//! impl LamellarAM for HelloWorld {
//!     async fn exec(self) {
//!         println!(
//!             "Hello World, I'm from PE {:?}",
//!             self.original_pe,
//!         );
//!     }
//! }
//!```
//! The [am] macro parses the provided implementation and performs a number of transformations to the code to enable execution of the active message.
//! This macro is responsible for generating the code which will perform serialization/deserialization of both the active message data and any returned data.
//!
//! Each active message implementation is assigned a unique ID at runtime initialization, these IDs are then used as the key
//! to a Map containing specialized deserialization functions that convert a slice of bytes into the appropriate data type on the remote PE.
//!
//! The final step is to actually launch an active message and await its result
//!```
//! # use lamellar::active_messaging::prelude::*;
//! # #[AmData(Debug,Clone)]
//! # struct HelloWorld {
//! #    original_pe: usize, //this will contain the ID of the PE this data originated from
//! # }
//! # #[lamellar::am]
//! # impl LamellarAM for HelloWorld {
//! #     async fn exec(self) {
//! #         println!(
//! #             "Hello World, I'm from PE {:?}",
//! #             self.original_pe,
//! #         );
//! #     }
//! # }
//! fn main(){
//!     let world = lamellar::LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     //Send a Hello World Active Message to all pes
//!     let request = world.exec_am_all(
//!         HelloWorld {
//!             original_pe: my_pe,
//!         }
//!     );
//!     //wait for the request to complete
//!     world.block_on(request);
//! }
//!```
//! In this example we simply send a `HelloWorld` from every PE to every other PE using `exec_am_all` (please see the [ActiveMessaging] trait documentation for further details).
//! `exec_am_all` returns a `Future` which we can use to await the completion of our operation.
//!
//! Sample output for the above example on a 2 PE system may look something like (exact ordering is nondeterministic due to asynchronous behavior)
//!```text
//! Hello World, I'm from PE 0
//! Hello World, I'm from PE 1
//! Hello World, I'm from PE 0
//! Hello World, I'm from PE 1
//!```
//!
//! What if we wanted to actuall know where we are currently executing?

//! # Lamellar AM DSL
//! This lamellar [am] macro also parses the provided code block for the presence of keywords from a small DSL, specifically searching for the following token streams:
//! - ```lamellar::current_pe``` - return the world id of the PE this active message is executing on
//! - ```lamellar::num_pes``` - return the number of PEs in the world
//! - ```lamellar::world``` - return a reference to the instantiated LamellarWorld
//! - ```lamellar::team``` - return a reference to the LamellarTeam responsible for launching this AM
//!
//! Given this functionality, we can adapt the above active message body to this:
//!```
//! # use lamellar::active_messaging::prelude::*;
//! # #[AmData(Debug,Clone)]
//! # struct HelloWorld {
//! #    original_pe: usize, //this will contain the ID of the PE this data originated from
//! # }
//! #[lamellar::am]
//! impl LamellarAM for HelloWorld {
//!     async fn exec(self) {
//!         println!(
//!             "Hello World on PE {:?} of {:?}, I'm from PE {:?}",
//!             lamellar::current_pe,
//!             lamellar::num_pes,
//!             self.original_pe,
//!         );
//!     }
//! }
//! # fn main(){
//! #     let world = lamellar::LamellarWorldBuilder::new().build();
//! #     let my_pe = world.my_pe();
//! #     //Send a Hello World Active Message to all pes
//! #     let request = world.exec_am_all(
//! #         HelloWorld {
//! #             original_pe: my_pe,
//! #         }
//! #     );
//! #     //wait for the request to complete
//! #     world.block_on(request);
//! # }
//!```
//! the new Sample output for the above example on a 2 PE system may look something like (exact ordering is nondeterministic due to asynchronous behavior)
//!```text
//! Hello World on PE 0 of 2, I'm from PE 0
//! Hello World on PE 0 of 2, I'm from PE 1
//! Hello World on PE 1 of 2, I'm from PE 0
//! Hello World on PE 1 of 2, I'm from PE 1
//!```
//! # Active Messages with return data
//! In the above examples, we simply launched a remote active message but did not return an result back to the originating PE.
//! Lamellar supports return both "plain old data"(as long as it impls [AmDist]) and other active messages themselves.
//!
//! ## Returning normal data
//! Lamellar Active Messages support returning data and it is as simple as specifying the return type in the implementation of the `exec` function.
//!```
//! # use lamellar::active_messaging::prelude::*;
//! # #[AmData(Debug,Clone)]
//! # struct HelloWorld {
//! #    original_pe: usize, //this will contain the ID of the PE this data originated from
//! # }
//! #[lamellar::am]
//! impl LamellarAM for HelloWorld {
//!     async fn exec(self) -> usize { //specify we are returning a usize
//!         println!(
//!             "Hello World on PE {:?} of {:?}, I'm from PE {:?}",
//!             lamellar::current_pe,
//!             lamellar::num_pes,
//!             self.original_pe,
//!         );
//!         lamellar::current_pe
//!     }
//! }
//! # fn main(){
//! #     let world = lamellar::LamellarWorldBuilder::new().build();
//! #     let my_pe = world.my_pe();
//! #     //Send a Hello World Active Message to all pes
//! #     let request = world.exec_am_all(
//! #         HelloWorld {
//! #             original_pe: my_pe,
//! #         }
//! #     );
//! #     //wait for the request to complete
//! #     world.block_on(request);
//! # }
//!```
//! Retrieving the result is as simple as assigning a variable to the awaited request
//!```
//! # use lamellar::active_messaging::prelude::*;
//! # #[AmData(Debug,Clone)]
//! # struct HelloWorld {
//! #    original_pe: usize, //this will contain the ID of the PE this data originated from
//! # }
//! # #[lamellar::am]
//! # impl LamellarAM for HelloWorld {
//! #     async fn exec(self) -> usize { //specify we are returning a usize
//! #         println!(
//! #             "Hello World on PE {:?} of {:?}, I'm from PE {:?}",
//! #             lamellar::current_pe,
//! #             lamellar::num_pes,
//! #             self.original_pe,
//! #         );
//! #         lamellar::current_pe
//! #     }
//! # }
//! fn main(){
//!     let world = lamellar::LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     //Send a Hello World Active Message to all pes
//!     let request = world.exec_am_all(
//!         HelloWorld {
//!             original_pe: my_pe,
//!         }
//!     );
//!     //wait for the request to complete
//!     let results = world.block_on(request);
//!     println!("PE {my_pe} {results:?}");
//! }
//!```
//! The new Sample output for the above example on a 2 PE system may look something like (exact ordering is nondeterministic due to asynchronous behavior)
//!```text
//! Hello World on PE 0 of 2, I'm from PE 0
//! Hello World on PE 0 of 2, I'm from PE 1
//! Hello World on PE 1 of 2, I'm from PE 0
//! Hello World on PE 1 of 2, I'm from PE 1
//! PE 0 [0,1]
//! PE 1 [0,1]
//!```
//! ## Returning Active Messages
//! Lamellar also provides the ability to return another active message as a result.
//!
//! This active message will execute automatically when it arrives back at the originating node as intended as a sort of callback mechanism.
//!
//! Returning an active messages requires a few more changes to our code.
//! First we will define our new active message
//!```
//! # use lamellar::active_messaging::prelude::*;
//! #[AmData(Debug,Clone)]
//! struct ReturnAm{
//!     original_pe: usize,
//!     remote_pe: usize,
//! }
//!
//! #[lamellar::am]
//! impl LamellarAm for ReturnAm{
//!     async fn exec(self) {
//!         println!("initiated on PE {} visited PE {} finishing on PE {}",self.original_pe,self.remote_pe,lamellar::current_pe);
//!     }
//! }
//!```
//! With that defined we can now modify our original Active Message to return this new `ReturnAm` type.
//! The main change is that we need to explicitly tell the macro we are returning an active message and we provide the name of the active message we are returning
//!```
//! # use lamellar::active_messaging::prelude::*;
//! # #[AmData(Debug,Clone)]
//! # struct ReturnAm{
//! #     original_pe: usize,
//! #     remote_pe: usize,
//! # }
//! # #[lamellar::am]
//! # impl LamellarAm for ReturnAm{
//! #     async fn exec(self) {
//! #         println!("initiated on PE {} visited PE {} finishing on PE {}",self.original_pe,self.remote_pe,lamellar::current_pe);
//! #     }
//! # }
//! # #[AmData(Debug,Clone)]
//! # struct HelloWorld {
//! #    original_pe: usize, //this will contain the ID of the PE this data originated from
//! # }
//! #[lamellar::am(return_am = "ReturnAm")] //we explicitly tell the macro we are returning an AM
//! impl LamellarAM for HelloWorld {
//!     async fn exec(self) -> usize { //specify we are returning a usize
//!         println!(
//!             "Hello World on PE {:?} of {:?}, I'm from PE {:?}",
//!             lamellar::current_pe,
//!             lamellar::num_pes,
//!             self.original_pe,
//!         );
//!         ReturnAm{ //simply return an instance of the AM
//!             original_pe: self.original_pe,
//!             remote_pe: lamellar::current_pe,
//!         }
//!     }
//! }
//! # fn main(){
//! #     let world = lamellar::LamellarWorldBuilder::new().build();
//! #     let my_pe = world.my_pe();
//! #     //Send a Hello World Active Message to all pes
//! #     let request = world.exec_am_all(
//! #         HelloWorld {
//! #             original_pe: my_pe,
//! #         }
//! #     );
//! #     //wait for the request to complete
//! #     let results = world.block_on(request);
//! #     println!("PE {my_pe} {results:?}");
//! # }
//!```
//! We do not need to modify any of the code in our main function, so the new Sample output for the above example on a 2 PE system may look something like (exact ordering is nondeterministic due to asynchronous behavior)
//!```text
//! Hello World on PE 0 of 2, I'm from PE 0
//! Hello World on PE 0 of 2, I'm from PE 1
//! Hello World on PE 1 of 2, I'm from PE 0
//! Hello World on PE 1 of 2, I'm from PE 1
//! initiated on PE 0 visited PE 0 finishing on PE 0
//! initiated on PE 0 visited PE 1 finishing on PE 0
//! initiated on PE 1 visited PE 0 finishing on PE 1
//! initiated on PE 1 visited PE 0 finishing on PE 1
//! PE 0 [(),()]
//! PE 1 [(),()]
//!```
//! By examining the above output we can see that printing the results of the request returns the unit type (well a Vector of unit types because of the `exec_am_all` call).
//! This is because our returned AM does not return any data itself.
//!
//! ## Returning Active Messages which return data
//! Lamellar does support returning an Active Message which then returns some data.
//! First we need to update `ReturnAm` to actually return some data
//!```
//! # use lamellar::active_messaging::prelude::*;
//! # #[AmData(Debug,Clone)]
//! # struct ReturnAm{
//! #     original_pe: usize,
//! #     remote_pe: usize,
//! # }
//!
//! #[lamellar::am]
//! impl LamellarAm for ReturnAm{
//!     async fn exec(self) -> (usize,usize) {
//!         println!("initiated on PE {} visited PE {} finishing on PE {}",self.original_pe,self.remote_pe,lamellar::current_pe);
//!         (self.original_pe,self.remote_pe)
//!     }
//! }
//!```
//! Next we need to make an additional change to the  `HelloWorld` am to specify that our returned am will return data itself.
//! we do this in the argument to the [am] procedural macro
//!```
//! # use lamellar::active_messaging::prelude::*;
//! # #[AmData(Debug,Clone)]
//! # struct ReturnAm{
//! #     original_pe: usize,
//! #     remote_pe: usize,
//! # }
//! # #[lamellar::am]
//! # impl LamellarAm for ReturnAm{
//! #     async fn exec(self) -> (usize,usize) {
//! #         println!("initiated on PE {} visited PE {} finishing on PE {}",self.original_pe,self.remote_pe,lamellar::current_pe);
//! #         (self.original_pe,self.remote_pe)
//! #     }
//! # }
//! # #[AmData(Debug,Clone)]
//! # struct HelloWorld {
//! #    original_pe: usize, //this will contain the ID of the PE this data originated from
//! # }
//!
//! #[lamellar::am(return_am = "ReturnAm -> (usize,usize)")] //we explicitly tell the macro we are returning an AM which itself returns data
//! impl LamellarAM for HelloWorld {
//!     async fn exec(self) -> usize { //specify we are returning a usize
//!         println!(
//!             "Hello World on PE {:?} of {:?}, I'm from PE {:?}",
//!             lamellar::current_pe,
//!             lamellar::num_pes,
//!             self.original_pe,
//!         );
//!         ReturnAm{ //simply return an instance of the AM
//!             original_pe: self.original_pe,
//!             remote_pe: lamellar::current_pe,
//!         }
//!     }
//! }
//! # fn main(){
//! #     let world = lamellar::LamellarWorldBuilder::new().build();
//! #     let my_pe = world.my_pe();
//! #     //Send a Hello World Active Message to all pes
//! #     let request = world.exec_am_all(
//! #         HelloWorld {
//! #             original_pe: my_pe,
//! #         }
//! #     );
//! #     //wait for the request to complete
//! #     let results = world.block_on(request);
//! #     println!("PE {my_pe} {results:?}");
//! # }
//!```
//! With those changes, the new Sample output for the above example on a 2 PE system may look something like (exact ordering is nondeterministic due to asynchronous behavior)
//!```text
//! Hello World on PE 0 of 2, I'm from PE 0
//! Hello World on PE 0 of 2, I'm from PE 1
//! Hello World on PE 1 of 2, I'm from PE 0
//! Hello World on PE 1 of 2, I'm from PE 1
//! initiated on PE 0 visited PE 0 finishing on PE 0
//! initiated on PE 0 visited PE 1 finishing on PE 0
//! initiated on PE 1 visited PE 0 finishing on PE 1
//! initiated on PE 1 visited PE 0 finishing on PE 1
//! PE 0 [(0,0),(0,1)]
//! PE 1 [(1,0),(1,1)]
//!```
//! # Nested Active Messages
//! Lamellar Active Messages support nested active messages, i.e launching a new active message from within an executing active message.
//!
//! This functionality can be used to setup active message dependencies, enable recursive active messages, ect.
//! In the following example we will construct a recursive active message that performs a ring like commincation pattern accross PEs, which
//! will return the reverse order in which it vistited the PE's.
//!```
//! use lamellar::active_messaging::prelude::*;
//! #[AmData(Debug,Clone)]
//! struct RingAm {
//!    original_pe: usize, //this will be are recursion terminating condition
//! }
//! #[lamellar::am]
//! impl LamellarAm for RingAm{
//!     async fn exec(self) -> Vec<usize>{
//!         let cur_pe = lamellar::current_pe;
//!         if self.original_pe ==  cur_pe{ //terminate the recursion!
//!             vec![cur_pe] //return a new path with the current_pe as the start
//!         }
//!         else { //launch another active message
//!             let next_pe = (cur_pe + 1 ) % lamellar::num_pes; //account for wrap arround
//!             let req = lamellar::team.exec_am_pe(next_pe, RingAm{original_pe: self.original_pe});//we can clone self because we don't need to modify any data
//!             let mut path = req.await; // exec_am_*() calls return a future we used to get the result from
//!             path.push(cur_pe); //update the path with the PE and return
//!             path
//!         }
//!     }
//! }
//!
//! fn main(){
//!     let world = lamellar::LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     let num_pes = world.num_pes();
//!     //Send initial message to right neighbor
//!     let next_pe = (my_pe + 1) % num_pes; //account for wrap arround
//!     let request = world.exec_am_pe(
//!         next_pe,
//!         RingAm {
//!             original_pe: my_pe
//!         }
//!     );
//!     //wait for the request to complete
//!     let results = world.block_on(request);
//!     println!("PE {my_pe} {results:?}");
//! }
//!```
//! The key thing to notice in this example is how we wait for a request to finish will change depending on the context we are executing in.
//! When we are in the active message we are already in an asychronous context so we can simply `await` the future returned to us by the `exec_am_pe()` call.
//! This is in contrast to the main function where we must use a `block_on` call to drive the future an retrieve the result.
//!
//! The sample output for the above example on a 4 PE system may look something like (exact ordering is nondeterministic due to asynchronous behavior)
//!```text
//! PE 0 [0,3,2,1]
//! PE 1 [1,0,3,2]
//! PE 2 [2,1,0,3]
//! PE 3 [3,2,1,0]
//!```
//! # Active Message Groups
//! Up until now, we have seen two extremes with respect to the granularity with which active messages can be awaited.
//! Either awaiting all outstanding active messages in the system via `wait_all()`, or awaiting an individual active message e.g. `req.await`.
//! Lamellar also supports active message groups, which is a collection of active messages that can be awaited together.
//! Conceptually, an active message group can be represented as a meta active message that contains a list of the actual active messages we want to execute,
//! as illustrated in the pseudocode below:
//! ```no_run
//! #[AmData(Debug,Clone)]
//! struct MetaAm{
//!     ams: Vec<impl LamellarAm>
//! }
//! #[lamellar::am]
//! impl LamellarAm for MetaAm{
//!     async fn exec(self) {
//!         for am in self.ams{
//!             am.exec().await
//!         }
//!     }
//! }
//! ```
//!
//! There are two flavors of active message groups discussed in the following sections:
//!
//! ## Generic Active Message Groups
//! The first Active Message Group is called [AmGroup](crate::lamellar_task_group::AmGroup) which can include any AM `AM: impl LamellarAm<Output=()>`.
//! That is, the active messages in the group can consists of different underlying types as long as they all return `()`.
//! Future implementations will relax this restriction, so that they only need to return the same type.
//! ```
//! use lamellar::active_messaging::prelude::*;
//! #[AmData(Debug,Clone)]
//! struct Am1 {
//!    foo: usize,
//! }
//! #[lamellar::am]
//! impl LamellarAm for RingAm{
//!     async fn exec(self) -> Vec<usize>{
//!         println!("in am1 {:?} on PE{:?}",self.foo,  lamellar::current_pe);
//!     }
//! }
//!
//! #[AmData(Debug,Clone)]
//! struct Am2 {
//!    bar: String,
//! }
//! #[lamellar::am]
//! impl LamellarAm for RingAm{
//!     async fn exec(self) -> Vec<usize>{
//!         println!("in am2 {:?} on PE{:?}",self.bar,lamellar::current_pe);
//!     }
//! }
//!
//! fn main(){
//!     let world = lamellar::LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     let num_pes = world.num_pes();
//!
//!     let am1 = Am1{foo: 1};
//!     let am2 = Am2{bar: "hello".to_string()};
//!     //create a new AMGroup
//!     let am_group = AMGroup::new(&world);
//!     // add the AMs to the group
//!     // we can specify individual PEs to execute on or all PEs
//!     am_group.add_am_pe(0,am1.clone());
//!     am_group.add_am_pe(1,am1.clone());
//!     am_group.add_am_pe(0,am2.clone());
//!     am_group.add_am_pe(1,am2.clone());
//!     am_group.add_am_all(am1.clone());
//!     am_group.add_am_all(am2.clone());
//!
//!     //execute and await the completion of all AMs in the group
//!     world.block_on(am_group.exec());
//! }
//!```
//! Expected output on each PE:
//! ```text
//! in am1 1 on PE0
//! in am2 hello on PE0
//! in am1 1 on PE0
//! in am2 hello on PE0
//! in am1 1 on PE1
//! in am2 hello on PE1
//! in am1 1 on PE1
//! in am2 hello on PE1
//! ```
//!  ## Typed Active Message Groups
//! The second Active Message Group is called `TypedAmGroup` which can only include AMs of a specific type (but this type can return data).
//! Data is returned in the same order as the AMs were added
//! (You can think of this as similar to `Vec<T>`)
//! Typed Am Groups are instantiated using the [typed_am_group] macro which expects two parameters, the first being the type (name) of the AM and the second being a reference to a lamellar team.
//! ```
//! use lamellar::active_messaging::prelude::*;
//! use lamellar::darc::prelude::*;
//! use std::sync::atomic::AtomicUsize;
//! #[AmData(Debug,Clone)]
//! struct ExampleAm {
//!    cnt: Darc<AtomicUsize>,
//! }
//! #[lamellar::am]
//! impl LamellarAm for ExampleAm{
//!     async fn exec(self) -> usize{
//!         self.cnt.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
//!     }
//! }
//!
//! fn main(){
//!     let world = lamellar::LamellarWorldBuilder::new().build();
//!     let my_pe = world.my_pe();
//!     let num_pes = world.num_pes();
//!
//!     if my_pe == 0 { // we only want to run this on PE0 for sake of illustration
//!         let am_group = typed_am_group!{ExampleAm,&world};
//!         let am = ExampleAm{cnt: 0};
//!         // add the AMs to the group
//!         // we can specify individual PEs to execute on or all PEs
//!         am_group.add_am_pe(0,am.clone());
//!         am_group.add_am_all(am.clone());
//!         am_group.add_am_pe(1,am.clone());
//!         am_group.add_am_all(am.clone());
//!
//!         //execute and await the completion of all AMs in the group
//!         let results = world.block_on(am_group.exec()); // we want to process the returned data
//!         //we can index into the results
//!         if let AmGroupResult::Pe((pe,val)) = results.at(2){
//!             assert_eq!(pe, 1); //the third add_am_* call in the group was to execute on PE1
//!             assert_eq!(val, 1); // this was the second am to execute on PE1 so the fetched value is 1
//!         }
//!         //or we can iterate over the results
//!         for res in results{
//!             match res{
//!                 AmGroupResult::Pe((pe,val)) => { println!("{} from PE{}",val,pe)},
//!                 AmGroupResult::All(val) => { println!("{} on all PEs",val)},
//!             }
//!         }
//!     }
//! }
//!```
//! Expected output on each PE1:
//! ```text
//! 0 from PE0
//! [1,0] on all PEs
//! 1 from PE1
//! [2,2] on all PEs
//! ```
//! ### Static Members
//! In the above code, the `ExampleAm` stuct contains a member that is a [Darc](crate::darc::Darc) (Distributed Arc).
//! In order to properly calculate distributed reference counts Darcs implements specialized Serialize and Deserialize operations.
//! While, the cost to any single serialization/deserialization operation is small, doing this for every active message containing
//! a Darc can become expensive.
//!
//! In certain cases Typed Am Groups can avoid the repeated serialization/deserialization of Darc members if the user guarantees
//! that every Active Message in the group is using a reference to the same Darc. In this case, we simply would only need
//! to serialize the Darc once for each PE it gets sent to.
//!
//! This can be accomplished by using the [AmData] attribute macro with the `static` keyword passed in as an argument as illustrated below:
//! ```
//! use lamellar::active_messaging::prelude::*;
//! use lamellar::darc::prelude::*;
//! use std::sync::atomic::AtomicUsize;
//! #[AmData(Debug,Clone)]
//! struct ExampleAm {
//!    #[AmData(static)]
//!    cnt: Darc<AtomicUsize>,
//! }
//!```
//! Other than the addition of `#[AmData(static)]` the rest of the code as the previous example would be the same.

use crate::darc::__NetworkDarc;
use crate::lamellae::{Lamellae, LamellaeRDMA, SerializedData};
use crate::lamellar_arch::IdError;
use crate::lamellar_request::{InternalResult, LamellarRequestResult};
use crate::lamellar_team::{LamellarTeam, LamellarTeamRT};
use crate::memregion::one_sided::NetMemRegionHandle;
use crate::scheduler::{Executor, LamellarExecutor, ReqId};
// use log::trace;
use async_trait::async_trait;
use futures::Future;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[doc(hidden)]
pub mod prelude;

pub(crate) mod registered_active_message;
use registered_active_message::RegisteredActiveMessages;
#[doc(hidden)]
pub use registered_active_message::RegisteredAm;

pub(crate) mod batching;

pub(crate) const BATCH_AM_SIZE: usize = 100_000;

/// This macro is used to setup the attributed type so that it can be used within remote active messages.
///
/// For this derivation to succeed all members of the data structure must impl [AmDist] (which it self is a blanket impl)
///
///```ignore
/// AmDist: serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send + 'static {}
/// impl<T: serde::ser::Serialize + serde::de::DeserializeOwned + Sync + Send + 'static> AmDist for T {}
///```
///
/// Typically you will use this macro in place of `#[derive()]`, as it will manage deriving both the traits
/// that are provided as well as those require by Lamellar for active messaging.
///
/// Generally this is paired with the [lamellar::am][am] macro on an implementation of the [LamellarAM], to associate a remote function with this data.
/// (if you simply want this type to able to be included in other active messages, implementing [LamellarAM] can be omitted )
///
/// When used to specify the data type of an AMit must be applied to the top of the struct definition.
///
/// Optionally, it can be applied to individual members of the struct
/// to specify that the given member is static with respect to a typed active message group ( [typed_am_group] ).
pub use lamellar_impl::AmData;

/// This macro is used to setup the attributed type so that it can be used within local active messages.
///
/// Typically you will use this macro in place of `#[derive()]`, as it will manage deriving both the traits
/// that are provided as well as those require by Lamellar for active messaging.
///
/// This macro relaxes the Serialize/Deserialize trait bounds required by the [AmData] macro
///
/// Generally this is paired with the [lamellar::local_am][local_am] macro on an implementation of the [LamellarAM], to associate a local function with this data.
/// (if you simply want this type to able to be included in other active messages, implementing [LamellarAM] can be omitted )
///
pub use lamellar_impl::AmLocalData;

#[doc(hidden)]
pub use lamellar_impl::AmGroupData;

/// This macro is used to associate an implemenation of [LamellarAM] for type that has used the [AmData] attribute macro
///
/// This essentially constructs and registers the Active Message with the runtime. It is responsible for ensuring all data
/// within the active message is properly serialize and deserialized, including any returned results.
///
/// Each active message implementation is assigned a unique ID at runtime initialization, these IDs are then used as the key
/// to a Map containing specialized deserialization functions that convert a slice of bytes into the appropriate data type on the remote PE.
/// Finally, a worker thread will call that deserialized objects `exec()` function to execute the actual active message.
///
/// Please see the [Active Messaging][crate::active_messaging] module level documentation for more details
///
pub use lamellar_impl::am;

/// This macro is used to associate an implemenation of [LamellarAM] for a data structure that has used the [AmLocalData] attribute macro
///
/// This essentially constructs and registers the Active Message with the runtime. (LocalAms *do not* perform any serialization/deserialization)
///
/// Please see the [Active Messaging][crate::active_messaging] module level documentation for more details
///
pub use lamellar_impl::local_am;

/// This macro is used to construct an am group of a single am type.
pub use lamellar_impl::typed_am_group;

/// Supertrait specifying `Sync` + `Send`
pub trait SyncSend: Sync + Send {}

impl<T: Sync + Send> SyncSend for T {}

/// Supertrait specifying a Type can be used in (remote)ActiveMessages
///
/// Types must impl [Serialize][serde::ser::Serialize], [Deserialize][serde::de::DeserializeOwned], and [SyncSend]
pub trait AmDist: serde::ser::Serialize + serde::de::DeserializeOwned + SyncSend + 'static {}

impl<T: serde::ser::Serialize + serde::de::DeserializeOwned + SyncSend + 'static> AmDist for T {}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub(crate) enum ExecType {
    Am(Cmd),
    Runtime(Cmd),
}

#[doc(hidden)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum RemotePtr {
    NetworkDarc(__NetworkDarc),
    NetMemRegionHandle(NetMemRegionHandle),
}

#[doc(hidden)]
pub trait DarcSerde {
    fn ser(&self, num_pes: usize, darcs: &mut Vec<RemotePtr>);
    fn des(&self, cur_pe: Result<usize, IdError>);
}

impl<T> DarcSerde for &T {
    fn ser(&self, _num_pes: usize, _darcs: &mut Vec<RemotePtr>) {}
    fn des(&self, _cur_pe: Result<usize, IdError>) {}
}

#[doc(hidden)]
pub trait LamellarSerde: SyncSend {
    fn serialized_size(&self) -> usize;
    fn serialize_into(&self, buf: &mut [u8]);
    fn serialize(&self) -> Vec<u8>;
}

#[doc(hidden)]
pub trait LamellarResultSerde: LamellarSerde {
    fn serialized_result_size(&self, result: &LamellarAny) -> usize;
    fn serialize_result_into(&self, buf: &mut [u8], result: &LamellarAny);
}

#[doc(hidden)]
pub trait RemoteActiveMessage: LamellarActiveMessage + LamellarSerde + LamellarResultSerde {
    fn as_local(self: Arc<Self>) -> LamellarArcLocalAm;
}

#[doc(hidden)]
pub trait LamellarActiveMessage: DarcSerde {
    fn exec(
        self: Arc<Self>,
        my_pe: usize,
        num_pes: usize,
        local: bool,
        world: Arc<LamellarTeam>,
        team: Arc<LamellarTeam>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = LamellarReturn> + Send>>;
    fn get_id(&self) -> &'static str;
}

#[doc(hidden)]
pub trait LamellarResultDarcSerde: LamellarSerde + DarcSerde + Sync + Send {}

pub(crate) type LamellarArcLocalAm = Arc<dyn LamellarActiveMessage + Sync + Send>;
pub(crate) type LamellarArcAm = Arc<dyn RemoteActiveMessage + Sync + Send>;
pub(crate) type LamellarAny = Box<dyn std::any::Any + Sync + Send>;
pub(crate) type LamellarResultArc = Arc<dyn LamellarResultDarcSerde + Sync + Send>;

/// Supertrait specifying `serde::ser::Serialize` + `serde::de::DeserializeOwned`
pub trait Serde: serde::ser::Serialize + serde::de::DeserializeOwned {}

/// The trait representing an active message that can only be executed locally, i.e. from the PE that initiated it
/// (SyncSend is a blanket impl for Sync + Send)
pub trait LocalAM: SyncSend {
    /// The type of the output returned by the active message
    type Output: SyncSend;
}

/// The trait representing an active message that can be executed remotely
/// (AmDist is a blanket impl for serde::Serialize + serde::Deserialize + Sync + Send + 'static)
#[async_trait]
pub trait LamellarAM {
    /// The type of the output returned by the active message
    type Output: AmDist;
    /// The function representing the work done by the active message
    async fn exec(self) -> Self::Output;
}

#[doc(hidden)]
pub enum LamellarReturn {
    LocalData(LamellarAny),
    LocalAm(LamellarArcAm),
    RemoteData(LamellarResultArc),
    RemoteAm(LamellarArcAm),
    Unit,
}

impl std::fmt::Debug for LamellarReturn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LamellarReturn::LocalData(_) => write!(f, "LocalData"),
            LamellarReturn::LocalAm(_) => write!(f, "LocalAm"),
            LamellarReturn::RemoteData(_) => write!(f, "RemoteData"),
            LamellarReturn::RemoteAm(_) => write!(f, "RemoteAm"),
            LamellarReturn::Unit => write!(f, "Unit"),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReqMetaData {
    pub(crate) src: usize,         //source pe
    pub(crate) dst: Option<usize>, // destination pe - team based pe id, none means all pes
    pub(crate) id: ReqId,          // id of the request
    pub(crate) lamellae: Arc<Lamellae>,
    pub(crate) world: Pin<Arc<LamellarTeamRT>>,
    pub(crate) team: Pin<Arc<LamellarTeamRT>>,
    pub(crate) team_addr: usize,
}

pub(crate) enum Am {
    All(ReqMetaData, LamellarArcAm),
    Remote(ReqMetaData, LamellarArcAm), //req data, am to execute
    Local(ReqMetaData, LamellarArcLocalAm), //req data, am to execute
    Return(ReqMetaData, LamellarArcAm), //req data, am to return and execute
    Data(ReqMetaData, LamellarResultArc), //req data, data to return
    Unit(ReqMetaData),                  //req data
}

impl std::fmt::Debug for Am {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Am::All(_, _) => write!(f, "All"),
            Am::Remote(_, _) => write!(f, "Remote"),
            Am::Local(_, _) => write!(f, "Local"),
            Am::Return(_, _) => write!(f, "Return"),
            Am::Data(_, _) => write!(f, "Data"),
            Am::Unit(_) => write!(f, "Unit"),
        }
    }
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Default,
)]
pub(crate) enum Cmd {
    #[default]
    Am, //a single am
    ReturnAm, //a single return am
    Data,     //a single data result
    Unit,     //a single unit result
    BatchedMsg, //a batched message, can contain a variety of am types
              // BatchedReturnAm, //a batched message, only containing return ams -- not sure this can happen
              // BatchedData, //a batched message, only containing data results
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, Default)]
pub(crate) struct Msg {
    pub src: u16,
    pub cmd: Cmd,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub(crate) enum RetType {
    //maybe change to ReqType? ReturnRequestType?
    Unit,
    Closure,
    Am,
    Data,
    Barrier,
    NoHandle,
    Put,
    Get,
}

#[derive(Debug)]
pub(crate) struct AMCounters {
    pub(crate) outstanding_reqs: Arc<AtomicUsize>,
    pub(crate) send_req_cnt: AtomicUsize,
}

impl AMCounters {
    pub(crate) fn new() -> AMCounters {
        AMCounters {
            outstanding_reqs: Arc::new(AtomicUsize::new(0)),
            send_req_cnt: AtomicUsize::new(0),
        }
    }
    pub(crate) fn add_send_req(&self, num: usize) {
        let _num_reqs = self.outstanding_reqs.fetch_add(num, Ordering::SeqCst);
        // println!("add_send_req {}",_num_reqs+1);
        self.send_req_cnt.fetch_add(num, Ordering::SeqCst);
    }
}

// pub trait LamellarExecutor {
//     fn spawn_task<F: Future>(&self, future: F) -> F::Output;
// }

/// The interface for launching, executing, and managing Lamellar Active Messages .
pub trait ActiveMessaging {
    #[doc(alias("One-sided", "onesided"))]
    /// launch and execute an active message on every PE (including originating PE).
    ///
    /// Expects as input an instance of a struct thats been defined using the lamellar::am procedural macros.
    ///
    /// Returns a future allow the user to poll for complete and retrive the result of the Active Message stored within a vector,
    /// each index in the vector corresponds to the data returned by the corresponding PE
    ///
    /// NOTE: lamellar active messages are not lazy, i.e. you do not need to drive the returned future to launch the computation,
    /// the future is only used to check for completion and/or retrieving any returned data
    ///
    /// # One-sided Operation
    /// The calling PE manages creating and transfering the active message to the remote PEs (without user intervention on the remote PEs).
    /// If a result is returned it will only be available on the calling PE.
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// #[lamellar::AmData(Debug,Clone)]
    /// struct Am{
    /// // can contain anything that impls Serialize, Deserialize, Sync, Send   
    ///     val: usize
    /// }
    ///
    /// #[lamellar::am]
    /// impl LamellarAM for Am{
    ///     async fn exec(self) -> usize { //can return nothing or any type that impls Serialize, Deserialize, Sync, Send
    ///         //do some remote computation
    ///         println!("hello from PE{}",self.val);
    ///         lamellar::current_pe //return the executing pe
    ///     }
    /// }
    /// //----------------
    ///
    /// let world = lamellar::LamellarWorldBuilder::new().build();
    /// let request = world.exec_am_all(Am{val: world.my_pe()}); //launch am on all pes
    /// let results = world.block_on(request); //block until am has executed and retrieve the data
    /// for i in 0..world.num_pes(){
    ///     assert_eq!(i,results[i]);
    /// }
    ///```
    fn exec_am_all<F>(&self, am: F) -> Pin<Box<dyn Future<Output = Vec<F::Output>> + Send>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist;

    #[doc(alias("One-sided", "onesided"))]
    /// launch and execute an active message on a specifc PE.
    ///
    /// Expects as input the PE to execute on and an instance of a struct thats been defined using the lamellar::am procedural macros.
    ///
    /// Returns a future allow the user to poll for complete and retrive the result of the Active Message
    ///
    ///
    /// NOTE: lamellar active messages are not lazy, i.e. you do not need to drive the returned future to launch the computation,
    /// the future is only used to check for completeion and/or retrieving any returned data
    ///
    /// # One-sided Operation
    /// The calling PE manages creating and transfering the active message to the remote PE (without user intervention on the remote PE).
    /// If a result is returned it will only be available on the calling PE.
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// #[lamellar::AmData(Debug,Clone)]
    /// struct Am{
    /// // can contain anything that impls Serialize, Deserialize, Sync, Send   
    ///     val: usize
    /// }
    ///
    /// #[lamellar::am]
    /// impl LamellarAM for Am{
    ///     async fn exec(self) -> usize { //can return nothing or any type that impls Serialize, Deserialize, Sync, Send
    ///         //do some remote computation
    ///         println!("hello from PE{}",self.val);
    ///         lamellar::current_pe //return the executing pe
    ///     }
    /// }
    /// //----------------
    ///
    /// let world = lamellar::LamellarWorldBuilder::new().build();
    /// let request = world.exec_am_pe(world.num_pes()-1, Am{val: world.my_pe()}); //launch am on all pes
    /// let result = world.block_on(request); //block until am has executed
    /// assert_eq!(world.num_pes()-1,result);
    ///```
    fn exec_am_pe<F>(&self, pe: usize, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: RemoteActiveMessage + LamellarAM + Serde + AmDist;

    #[doc(alias("One-sided", "onesided"))]
    /// launch and execute an active message on the calling PE.
    ///
    /// Expects as input an instance of a struct thats been defined using the lamellar::local_am procedural macros.
    ///
    /// Returns a future allow the user to poll for complete and retrive the result of the Active Message.
    ///
    ///
    /// NOTE: lamellar active messages are not lazy, i.e. you do not need to drive the returned future to launch the computation,
    /// the future is only used to check for completeion and/or retrieving any returned data.
    ///
    /// # One-sided Operation
    /// The calling PE manages creating and executing the active message local (remote PEs are not involved).
    /// If a result is returned it will only be available on the calling PE.
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    /// use parking_lot::Mutex;
    /// use std::sync::Arc;
    ///
    /// #[lamellar::AmLocalData(Debug,Clone)]
    /// struct Am{
    /// // can contain anything that impls Sync, Send  
    ///     val: Arc<Mutex<f32>>,
    /// }
    ///
    /// #[lamellar::local_am]
    /// impl LamellarAM for Am{
    ///     async fn exec(self) -> usize { //can return nothing or any type that impls Serialize, Deserialize, Sync, Send
    ///         //do some  computation
    ///         let mut val = self.val.lock();
    ///         *val += lamellar::current_pe as f32;
    ///         lamellar::current_pe //return the executing pe
    ///     }
    /// }
    /// //----------------
    ///
    /// let world = lamellar::LamellarWorldBuilder::new().build();
    /// let request = world.exec_am_local(Am{val: Arc::new(Mutex::new(0.0))}); //launch am locally
    /// let result = world.block_on(request); //block until am has executed
    /// assert_eq!(world.my_pe(),result);
    ///```
    fn exec_am_local<F>(&self, am: F) -> Pin<Box<dyn Future<Output = F::Output> + Send>>
    where
        F: LamellarActiveMessage + LocalAM + 'static;

    #[doc(alias("One-sided", "onesided"))]
    /// blocks calling thread until all remote tasks (e.g. active mesages, array operations)
    /// initiated by the calling PE have completed.
    ///
    /// # One-sided Operation
    /// this is not a distributed synchronization primitive (i.e. it has no knowledge of a Remote PEs tasks), the calling thread will only wait for tasks
    /// to finish that were initiated by the calling PE itself
    ///
    /// # Examples
    ///```
    /// # use lamellar::active_messaging::prelude::*;
    /// #
    /// # #[lamellar::AmData(Debug,Clone)]
    /// # struct Am{
    /// # // can contain anything that impls Sync, Send  
    /// #     val: usize,
    /// # }
    ///
    /// # #[lamellar::am]
    /// # impl LamellarAM for Am{
    /// #     async fn exec(self) -> usize { //can return nothing or any type that impls Serialize, Deserialize, Sync, Send
    /// #         //do some remote computation
    /// #          println!("hello from PE{}",self.val);
    /// #         lamellar::current_pe //return the executing pe
    /// #     }
    /// # }
    /// #
    /// # let world = lamellar::LamellarWorldBuilder::new().build();
    /// world.exec_am_all(Am{val: world.my_pe()});
    /// world.wait_all(); //block until the previous am has finished
    ///```
    fn wait_all(&self);

    #[doc(alias = "Collective")]
    /// Global synchronization method which blocks calling thread until all PEs in the barrier group (e.g. World, Team, Array) have entered
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the ActiveMessaging object to enter the barrier, otherwise deadlock will occur
    ///
    /// # Examples
    ///```
    /// use lamellar::active_messaging::prelude::*;
    ///
    /// let world = lamellar::LamellarWorldBuilder::new().build();
    /// //do some work
    /// world.barrier(); //block until all PEs have entered the barrier
    ///```
    fn barrier(&self);

    fn async_barrier(&self) -> impl Future<Output = ()> + Send;

    #[doc(alias("One-sided", "onesided"))]
    /// Run a future to completion on the current thread
    ///
    /// This function will block the caller until the given future has completed, the future is executed within the Lamellar threadpool
    ///
    /// Users can await any future, including those returned from lamellar remote operations
    ///
    /// # One-sided Operation
    /// this is not a distributed synchronization primitive and only blocks the calling thread until the given future has completed on the calling PE
    ///
    /// # Examples
    ///```no_run  
    /// # use lamellar::active_messaging::prelude::*;
    /// use async_std::fs::File;
    /// use async_std::prelude::*;
    /// # #[lamellar::AmData(Debug,Clone)]
    /// # struct Am{
    /// # // can contain anything that impls Sync, Send  
    /// #     val: usize,
    /// # }
    /// #
    /// # #[lamellar::am]
    /// # impl LamellarAM for Am{
    /// #     async fn exec(self) -> usize { //can return nothing or any type that impls Serialize, Deserialize, Sync, Send
    /// #         //do some remote computation
    /// #          println!("hello from PE{}",self.val);
    /// #         lamellar::current_pe //return the executing pe
    /// #     }
    /// # }
    /// #
    /// # let world = lamellar::LamellarWorldBuilder::new().build();
    /// # let num_pes = world.num_pes();
    /// let request = world.exec_am_all(Am{val: world.my_pe()}); //launch am locally
    /// let result = world.block_on(request); //block until am has executed
    /// // you can also directly pass an async block
    /// let world_clone = world.clone();
    /// world.block_on(async move {
    ///     let mut file = async_std::fs::File::open("a.txt").await.unwrap();
    ///     let mut buf = vec![0u8;1000];
    ///     for pe in 0..num_pes{
    ///         let data = file.read(&mut buf).await.unwrap();
    ///         world_clone.exec_am_pe(pe,Am{val: data}).await;
    ///     }
    ///     world_clone.exec_am_all(Am{val: buf[0] as usize}).await;
    /// });
    ///```
    fn block_on<F: Future>(&self, f: F) -> F::Output;
}

#[async_trait]
pub(crate) trait ActiveMessageEngine {
    async fn process_msg(self, am: Am, stall_mark: usize, immediate: bool);

    async fn exec_msg(self, msg: Msg, ser_data: SerializedData, lamellae: Arc<Lamellae>);

    fn get_team_and_world(
        &self,
        pe: usize,
        team_addr: usize,
        lamellae: &Arc<Lamellae>,
    ) -> (Arc<LamellarTeam>, Arc<LamellarTeam>) {
        let local_team_addr = lamellae.local_addr(pe, team_addr);
        let team_rt = unsafe {
            let team_ptr = local_team_addr as *mut *const LamellarTeamRT;
            // println!("{:x} {:?} {:?} {:?}", team_hash,team_ptr, (team_hash as *mut (*const LamellarTeamRT)).as_ref(), (*(team_hash as *mut (*const LamellarTeamRT))).as_ref());
            Arc::increment_strong_count(*team_ptr);
            Pin::new_unchecked(Arc::from_raw(*team_ptr))
        };
        let world_rt = if let Some(world) = team_rt.world.clone() {
            world
        } else {
            team_rt.clone()
        };
        let world = LamellarTeam::new(None, world_rt, true);
        let team = LamellarTeam::new(Some(world.clone()), team_rt, true);
        (team, world)
    }

    fn send_data_to_user_handle(&self, req_id: ReqId, pe: usize, data: InternalResult) {
        // println!("returned req_id: {:?}", req_id);
        let req = unsafe { Arc::from_raw(req_id.id as *const LamellarRequestResult) };
        // println!("strong count recv: {:?} ", Arc::strong_count(&req));
        req.add_result(pe, req_id.sub_id, data);
    }
}

// #[derive(Debug)]
// pub(crate) enum ActiveMessageEngineType {
//     RegisteredActiveMessages(RegisteredActiveMessages),
// }

// #[async_trait]
// impl ActiveMessageEngine for ActiveMessageEngineType {
//     async fn process_msg(
//         self,
//         am: Am,
//         executor: Arc<Executor>,
//         stall_mark: usize,
//         immediate: bool,
//     ) {
//         match self {
//             ActiveMessageEngineType::RegisteredActiveMessages(remote_am) => {
//                 remote_am
//                     .process_msg(am, executor, stall_mark, immediate)
//                     .await;
//             }
//         }
//     }
//     async fn exec_msg(
//         self,
//         msg: Msg,
//         ser_data: SerializedData,
//         lamellae: Arc<Lamellae>,
//         executor: Arc<Executor>,
//     ) {
//         match self {
//             ActiveMessageEngineType::RegisteredActiveMessages(remote_am) => {
//                 remote_am.exec_msg(msg, ser_data, lamellae, executor).await;
//             }
//         }
//     }
// }
