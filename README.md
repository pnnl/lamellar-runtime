Lamellar - Rust HPC runtime
=================================================

Lamellar is an asynchronous tasking runtime for HPC systems developed in RUST

SUMMARY
-------

Lamellar is an investigation of the applicability of the Rust systems programming language for HPC as an alternative to C and C++, with a focus on PGAS approaches.

Lamellar provides several different communication patterns to distributed applications. 
First, Lamellar allows for sending and executing active messages on remote nodes in a distributed environments. 
The runtime supports two forms of active messages:
The first method works with Stable rust and requires the user the register the active message by implementing a runtime exported trait (LamellarAM) and calling a procedural macro (\#[lamellar::am]) on the implementation.
The second method only works on nightly, but allows users to write serializable closures that are transfered and exectued by the runtime without registration 
It also exposes the concept of remote memory regions, i.e. allocations of memory that can read/written into by remote nodes.

Lamellar relies on network providers called Lamellae to perform the transfer of data throughout the system.
Currently two such Lamellae exist, one used for single node development purposed ("local"), and another based on the Rust OpenFabrics Interface Transport Layer (ROFI) (https://github.com/pnnl/rofi)

NEWS
----
* September 2020: Add support for "local" lamellae, prep for crates.io release -- v0.2.1
* July 2020: Second alpha release -- v0.2
* Feb 2020: First alpha release -- v0.1

EXAMPLES
--------

# Selecting a Lamellae and constructing a lamellar world instance
```rust
use lamellar::Backend;
fn main(){
 let mut world = lamellar::LamellarWorldBuilder::new()
        .with_lamellae( Default::default() ) //if "enable-rofi" feature is active default is rofi, otherwise  default is local
        //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend
        //.with_lamellae( Backend::Local )
        .build();
}
```

# Creating and executing a Registered Active Message
```rust
use lamellar::{ActiveMessaging, LamellarAm};
#[derive(serde::Serialize, serde::Deserialize)] 
struct HelloWorld { //the "input data" we are sending with our active message
    my_pe: usize, // "pe" is processing element == a node
}

#[lamellar::am]
impl LamellarAM for HelloWorld {
    fn exec(&self) {
        println!(
            "Hello pe {:?} of {:?}, I'm pe {:?}",
            lamellar::current_pe, 
            lamellar::num_pes,
            self.my_pe
        );
    }
}

fn main(){
    let mut world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let am = HelloWorld { my_pe: my_pe };
    for pe in 0..num_pes{
        world.exec_am_pe(pe,am.clone()); // explicitly launch on each PE
    }
    world.wait_all(); // wait for all active messages to finish
    world.barrier();  // synchronize with other pes
    let handle = world.exec_all(am.clone()); //also possible to execute on every PE with a single call
    handle.get(); //both exec_all and exec_am_pe return request handles that can be used to access any returned result
}
```



BUILD REQUIREMENTS
------------------


* Crates listed in Cargo.toml
    * Cargo.lock contains tested and working versions of dependencies


Optional:
Lamellar requires the following dependencies if wanting to run in a distributed HPC environment:
the rofi lamellae is enabled by adding "enable-rofi" to features either in cargo.toml or the command line when building. i.e. cargo build --features enable-rofi

* [ROFI](https://gitlab.pnnl.gov/lamellar/rofi)
* [rofi-sys](https://gitlab.pnnl.gov/lamellar/rofi-sys) -- available in [crates.io](https://crates.io/crates/rofisys)



To enable support for serializable remote closures compile with the nightly compiler and specify the "nightly" feature i.e. cargo build --features nightly

* RUST nightly compiler with the following features (enables remote closure API)
    * #![feature(unboxed_closures)]




At the time of release, Lamellar has been tested with the following external packages:

| **GCC** | **CLANG** | **ROFI**  | **OFI**   | **IB VERBS**  | **MPI**       | **SLURM** |
|--------:|----------:|----------:|----------:|--------------:|--------------:|----------:|
| 7.1.0   | 8.0.1     | 0.1.0     | 1.9.0     | 1.13          | mvapich2/2.3a | 17.02.7   |

The OFI_DIR environment variable must be specified with the location of the OFI installation.
The ROFI_DIR environment variable must be specified with the location of the ROFI installation.

BUILDING PACKAGE
----------------
In the following, assume a root directory ${ROOT}
0. download Lamellar to ${ROOT}/lamellar-runtime
    `cd ${ROOT} && git clone https://github.com/pnnl/lamellar-runtime`

0. download rofi-sys to ${ROOT}/rofi-sys -- or update Cargo.toml to point to the proper location
    `cd ${ROOT} && git clone https://github.com/pnnl/rofi-sys`

1. Select Lamellae to use

    In Cargo.toml add "enable-rofi" feature in wanting to use rofi, otherwise local lamellae will be used
    it may also be necessary to adjust the symmetric heap size (const MEM_SIZE) in rofi_lamellae.rs on the available memory in your system

2. Compile Lamellar lib and test executable (feature flags can be passed to command line instead of specifying in cargo.toml)

`cargo build (--release) (--features enable-rofi) (--features nightly)`

    executables located at ./target/debug(release)/test

3. Compile Examples

`cargo build --examples (--release) (--features enable-rofi) (--features nightly)`

    executables located at ./target/debug(release)/examples/

    Note: we do an explicit build instead of `cargo run --examples` as they are intended to run in a distriubted envrionment (see TEST section below.)


TESTING
-------
The examples are designed to be run with on at least two compute nodes, but they will work on a single node using the "local" lamellae. Here is a simple proceedure to run the tests that assume a compute cluster and [SLURM](https://slurm.schedmd.com) job manager. Please, refer to the job manager documentaiton for details on how to run command on different clusters. Lamellar grabs job information (size, distribution, etc.) from the jbo manager and runtime launcher (e.g., MPI, please refer to the BUILING REQUIREMENTS section for a list of tested software versions).

1. Allocates two compute nodes on the cluster:

`salloc -N 2 -p partition_name`

2. Run Lamellar test using `mpiexec` launcher.

`mpiexec -n 2 ./target/release/test` 
runs a simple acitve message based bandwidth test

3. Run lamellar examples

`mpiexec -n 2 ./target/release/examples/{example}` 
where `<test>` in {`all_to_all, array_put, array_static, array, get_bw, put_bw, hello, return`}. 

or alternatively:

`srun -N 2 -p partition_name -mpi=pmi2 ./target/release/examples/{example}` 
where `<test>` in {`all_to_all, array_put, array_static, array, get_bw, put_bw, hello, return, dft_proxy`}

Finally, the number of worker threads used within lamellar is controlled by setting and environment variable: LAMELLAR_THREADS

e.g. `export LAMELLAR_THREADS=10`

Note, if running on a single node, simple execute the binaries directly, no need to use mpiexec or srun.


HISTORY
-------
- version 0.2.1:
  - Provide the local lamellae as the default lamellae
  - feature guard rofi lamellae so that lamellar can build on systems without libfabrics and ROFI
  - added an example proxy app for doing a distributed dft
- version 0.2:
  - New user facing API
  - Registered Active Messages (enabling stable rust)
  - Remote Closures feature guarded for use with nightly rust
  - redesigned internal lamellae organization
  - initial support for world and teams (sub groups of pe)
- version 0.1:
  - Basic init/finit functionalities
  - Remote Closure Execution
  - Basic memory management (heap and data section)
  - Basic Remote Memory Region Support (put/get)
  - ROFI Lamellae (Remote Closure Execution, Remote Memory Regions)
  - Sockets Lamellae (Remote Closure Execution, limited support for Remote Memory Regions)
  - simple examples
  
NOTES
-----

STATUS
------
Lamellar is still under development, thus not all intended features are yet
implemented.

CONTACTS
--------
Ryan Friese     - ryan.friese@pnnl.gov  
Roberto Gioiosa - roberto.gioiosa@pnnl.gov  
Mark Raugas     - mark.raugas@pnnl.gov  

## License

This project is licensed under the BSD License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

This work was supported by the High Performance Data Analytics (HPDA) Program at Pacific Northwest National Laboratory (PNNL),
a multi-program DOE laboratory operated by Battelle.
