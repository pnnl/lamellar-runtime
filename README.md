Lamellar - Rust HPC runtime
=================================================

Lamellar is an asynchronous tasking runtime for HPC systems developed in RUST

SUMMARY
-------

Lamellar is an investigation of the applicability of the Rust systems programming language for HPC as an alternative to C and C++, with a focus on PGAS approaches.

Lamellar provides several different communication patterns to distributed applications. 
First, Lamellar allows for sending an executing closures (active messages) on remote nodes in a distributed environments. 
It also exposes the concept of remote memory regions, i.e. allocations of memory that can read/written into by remote nodes.

Lamellar relies and network provides called Lamellae to perform the transfer of data through out the system.
Currently two such Lamellae exist, one based on Rust standard libray sockets, and another based on the Rust OpenFabrics Interface Transport Layer (ROFI) (https://github.com/pnnl/rofi)

NEWS
----
Feb 2020: First alpha release

BUILD REQUIREMENTS
------------------
Lamellar requires the following dependencies:

* [ROFI](https://github.com/pnnl/rofi)
* [rofi-sys](https://github.com/pnnl/rofi-sys)
* RUST nightly compiler with the following features
    * #![feature(unboxed_closures)]
    * #![feature(vec_into_raw_parts)]

* Crates listed in Cargo.toml
    * Cargo.lock contains tested and working versions of dependencies



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

    In Cargo.toml update the default variable with one of "SocketsBackend","RofiBackend"
    it may also be necessary to adjust the symmetric heap size (const MEM_SIZE) in rofi_lamellae.rs and/or sockets_lamellae.rs depending you the available memory in your system

2. Compile Lamellar lib and test executable

`cargo build (--release)`

    executables located at ./target/debug(release)/test

3. Compile Examples

`cargo build --examples (--release)`

    executables located at ./target/debug(release)/examples/

    Note: we do an explicit build instead of `cargo run --examples` as they are intended to run in a distriubted envrionment (see TEST section below.)


TESTING
-------
The examples are designed to be run with on at least two compute nodes. Here is a simple proceedure to run the tests that assume a compute cluster and [SLURM](https://slurm.schedmd.com) job manager. Please, refer to the job manager documentaiton for details on how to run command on different clusters. Lamellar grabs job information (size, distribution, etc.) from the jbo manager and runtime launcher (e.g., MPI, please refer to the BUILING REQUIREMENTS section for a list of tested software versions).

1. Allocates two compute nodes on the cluster:

`salloc -N 2 -p compute`

2. Run Lamellar test using `mpiexec` launcher.

`mpiexec -n 2 ./target/release/test` 
runs a simple acitve message based bandwidth test

3. Run lamellar examples

`mpiexec -n 2 ./target/release/examples/{example}` 
where `<test>` in {`all_to_all, array_put, array_static, array, get_bw, put_bw, hello, return`}. 


HISTORY
-------
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
