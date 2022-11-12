Lamellar - Rust HPC runtime
=================================================

Lamellar is an asynchronous tasking runtime for HPC systems developed in RUST

SUMMARY
-------

Lamellar is an investigation of the applicability of the Rust systems programming language for HPC as an alternative to C and C++, with a focus on PGAS approaches.

Lamellar provides several different communication patterns to distributed applications.

First, Lamellar allows for sending and executing user defined active messages on remote nodes in a distributed environments.
User first implement runtime exported trait (LamellarAM) for their data structures and then call a procedural macro (\#[lamellar::am]) on the implementation.
The procedural macro procudes all the nescessary code to enable remote execution of the active message.

Lamellar also provides PGAS capabilities through multiple interfaces.
The first is a low level interface for constructing memory regions which are readable and writable from remote pes (nodes).

The second is a high-level abstraction of distributed arrays, allowing for distributed iteration and data parallel processing of elements.

Lamellar relies on network providers called Lamellae to perform the transfer of data throughout the system.
Currently three such Lamellae exist, one used for single node (single process) development ("local"), , one used for single node (multi-process) development ("shmem") useful for emulating distributed environments,and another based on the Rust OpenFabrics Interface Transport Layer (ROFI) (https://github.com/pnnl/rofi).

NEWS
----
* November 2022: Alpha release -- v0.5
* March 2022: Alpha release -- v0.4
* April 2021: Alpha release -- v0.3
* September 2020: Add support for "local" lamellae, prep for crates.io release -- v0.2.1
* July 2020: Second alpha release -- v0.2
* Feb 2020: First alpha release -- v0.1

EXAMPLES
--------

# Selecting a Lamellae and constructing a lamellar world instance
```rust
use lamellar::Backend;
fn main() {
    let world = lamellar::LamellarWorldBuilder::new()
        .with_lamellae(Default::default()) //if "enable-rofi" feature is active default is rofi, otherwise  default is local
        //.with_lamellae( Backend::Rofi ) //explicity set the lamellae backend to rofi, using the provider specified by the LAMELLAR_ROFI_PROVIDER env var ("verbs" or "shm")
        .build();

    let num_pes = world.num_pes();
    let my_pe = world.my_pe();

    println!("num_pes {:?}, my_pe {:?}", num_pes, my_pe);
}
```

# HelloWorld -- Creating and executing a Registered Active Message
```rust
use lamellar::ActiveMessaging;

#[lamellar::AmData(Debug, Clone)]
struct HelloWorld {
    //the "input data" we are sending with our active message
    my_pe: usize, // "pe" is processing element == a node
}

#[lamellar::am]
impl LamellarAM for HelloWorld {
    fn exec(&self) {
        println!(
            "Hello pe {:?} of {:?}, I'm from pe {:?}",
            lamellar::current_pe,
            lamellar::num_pes,
            self.my_pe
        );
    }
}

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let am = HelloWorld { my_pe: my_pe };
    for pe in 0..num_pes {
        world.exec_am_pe(pe, am.clone()); // explicitly launch on each PE
    }
    world.wait_all(); // wait for all active messages to finish
    world.barrier(); // synchronize with other pes
    let handle = world.exec_am_all(am.clone()); //also possible to execute on every PE with a single call
    handle.get(); //both exec_am_all and exec_am_pe return request handles that can be used to access any returned result
}
```

# Creating, initializing, and iterating through a distributed array
```rust
use lamellar::array::{DistributedIterator,Distribution, OneSidedIterator, UnsafeArray};

fn main() {
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let block_array = UnsafeArray::<usize>::new(world.team(), num_pes * 5, Distribution::Block); //we also support Cyclic distribution.
    block_array
        .dist_iter_mut()
        .for_each(move |elem| *elem = my_pe); //simultaneosuly initialize array accross all pes, each pe only updates its local data
    block_array.wait_all();
    block_array.barrier();
    block_array.print();
    if my_pe == 0 {
        for (i, elem) in block_array.onesided_iter().into_iter().enumerate() {
            //iterate through entire array on pe 0 (automatically transfering remote data)
            println!("i: {} = {}", i, elem);
        }
    }
}
```

A number of more complete examples can be found in the examples folder. Sub directories loosely group examples by the feature they are illustrating

USING LAMELLAR
--------------
Lamellar is capable of running on single node workstations as well as distributed HPC systems.
For a workstation, simply copy the following to the dependency section of you Cargo.toml file:

``` lamellar = "0.4"```

If planning to use within a distributed HPC system a few more steps maybe necessessary (this also works on single workstations):

1. ensure Libfabric (with support for the verbs provider) is installed on your system (https://github.com/ofiwg/libfabric) 
2. set the OFI_DIR envrionment variable to the install location of Libfabric, this directory should contain both the following directories:
    * lib
    * include
3. copy the following to your Cargo.toml file:

```lamellar = { version = "0.4", features = ["enable-rofi"]}```


For both envrionments, build your application as normal

```cargo build (--release)```

BUILD REQUIREMENTS
------------------


* Crates listed in Cargo.toml


Optional:
Lamellar requires the following dependencies if wanting to run in a distributed HPC environment:
the rofi lamellae is enabled by adding "enable-rofi" to features either in cargo.toml or the command line when building. i.e. cargo build --features enable-rofi
Rofi can either be built from source and then setting the ROFI_DIR environment variable to the Rofi install directory, or by letting the rofi-sys crate build it automatically.

* [libfabric](https://github.com/ofiwg/libfabric) 
* [ROFI](https://github.com/pnnl/rofi)
* [rofi-sys](https://github.com/pnnl/rofi-sys) -- available in [crates.io](https://crates.io/crates/rofisys)





At the time of release, Lamellar has been tested with the following external packages:

| **GCC** | **CLANG** | **ROFI**  | **OFI**       | **IB VERBS**  | **MPI**       | **SLURM** |
|--------:|----------:|----------:|--------------:|--------------:|--------------:|----------:|
| 7.1.0   | 8.0.1     | 0.1.0     | 1.9.0 -1.14.0 | 1.13          | mvapich2/2.3a | 17.02.7   |

The OFI_DIR environment variable must be specified with the location of the OFI (libfabrics) installation.
The ROFI_DIR environment variable must be specified with the location of the ROFI installation (otherwise rofi-sys crate will build for you automatically).
(See https://github.com/pnnl/rofi for instructions installing ROFI (and libfabrics))

BUILDING PACKAGE
----------------
In the following, assume a root directory ${ROOT}

0. download Lamellar to ${ROOT}/lamellar-runtime

 `cd ${ROOT} && git clone https://github.com/pnnl/lamellar-runtime`

1. Select Lamellae to use:
    * In Cargo.toml add "enable-rofi" feature if wanting to use rofi (or pass --features enable-rofi to your cargo build command ), otherwise only support for local and shmem backends will be built.

2. Compile Lamellar lib and test executable (feature flags can be passed to command line instead of specifying in cargo.toml)

`cargo build (--release) (--features enable-rofi)`

    executables located at ./target/debug(release)/test

3. Compile Examples

`cargo build --examples (--release) (--features enable-rofi) `

    executables located at ./target/debug(release)/examples/

Note: we do an explicit build instead of `cargo run --examples` as they are intended to run in a distriubted envrionment (see TEST section below.)


TESTING
-------
The examples are designed to be run  with at least 2 processes (via either ROFI or shmem backends), but most will work on with a single process using the "local" lamellae. Here is a simple proceedure to run the tests that assume a compute cluster and [SLURM](https://slurm.schedmd.com) job manager. Please, refer to the job manager documentation for details on how to run commands on different clusters. Lamellar grabs job information (size, distribution, etc.) from the job manager and runtime launcher (e.g., MPI, please refer to the BUILDING REQUIREMENTS section for a list of tested software versions).

Lamellar Instructions (multi process, multi node)
---------------------------------------------

1. Allocates two compute nodes on the cluster:

`salloc -N 2 -p partition_name` 

2. Run lamellar examples

`mpiexec -n 2 ./target/release/examples/{example}` 
where `<example>` is the same name as the Rust filenames in each subdirectory in the examples folder (e.g. "am_no_return")

or alternatively:

`srun -N 2 -p partition_name -mpi=pmi2 ./target/release/examples/{example}` 
where `<example>` is the same name as the Rust filenames in each subdirectory in the examples folder  (e.g. "am_no_return")

Shmem (multi-process, single node)
----------------------------------

1. use the lamellar_run.sh script to launch application

`./lamellar_run -N=2 -T=10 ./target/release/examples/{example}`
where `<example>` is the same name as the Rust filenames in each subdirectory in the examples folder  (e.g. "am_no_return")
N = the number of processes, T = the number of worker threads

Local (single-process, single node)
-----------------------------------
1. directly launch the executable

`./target/release/examples/{example}`
where `<example>` is the same name as the Rust filenames in each subdirectory in the examples folder  (e.g. "am_no_return")


ENVIRONMENT VARIABLES
---------------------
The number of worker threads used within lamellar is controlled by setting an environment variable: LAMELLAR_THREADS
e.g. `export LAMELLAR_THREADS=10`


The default backend used during an execution can be set using the LAMELLAE_BACKEND environment variable. Note that if a backend is explicity set in the world builder, this variable is ignored.
current the variable can be set to:
`local` -- single-process, single-node
`shmem` -- multi-process, single-node
`rofi` -- multi-process, multi-node


Internally, Lamellar utilizes memory pools of RDMAable memory for internal Runtime data structures and buffers, this allocation pool is also used to construct `LamellarLocalMemRegions` (as this operation does not require communication with other PE's). Additional memory pools are dynamically allocated accross the system as needed. This can be a fairly expensive operation (as the operation is synchronous across all pes) so the runtime will print a message at the end of execution with how many additional pools were allocated. 
To alleviate potential impacts to performance lamellar exposes the `LAMELLAR_MEM_SIZE` environment variable to set the default size of these allocation pools. 
The default size is 1GB per process.
For example, setting to 20GB could be accomplished with `LAMELLAR_MEM_SIZE=$((20*1024*1024*1024))`.
Note that when using the `shmem` backend the total allocated on the local node is `LAMELLAR_MEM_SIZE * number of processes`


HISTORY
-------
- version 0.4
  - Distributed Arcs (Darcs: distributed atomically reference counted objects)
  - LamellarArrays
    - UnsafeArray, AtomicArray, LocalLockArray, ReadOnlyArray, LocalOnlyArray
    - Distributed Iteration
    - Local Iteration
  - SHMEM backend
  - dynamic internal RDMA memory pools 
- version 0.3.0
  - recursive active messages
  - subteam support
  - support for custom team architectures (Examples/team_examples/custom_team_arch.rs)
  - initial support of LamellarArray (Am based collectives on distributed arrays)
  - integration with Rofi 0.2
  - revamped examples
- version 0.2.2:
  - Provide examples in readme
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
Erdal Mutlu     - erdal.mutlu@pnnl.gov  
Joseph Cottam   - joseph.cottam@pnnl.gov
Mark Raugas     - mark.raugas@pnnl.gov  

## License

This project is licensed under the BSD License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

This work was supported by the High Performance Data Analytics (HPDA) Program at Pacific Northwest National Laboratory (PNNL),
a multi-program DOE laboratory operated by Battelle.
