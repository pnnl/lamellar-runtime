Lamellar - Rust HPC runtime
=================================================

Lamellar is an investigation of the applicability of the Rust systems programming language for HPC as an alternative to C and C++, with a focus on PGAS approaches.

# Some Nomenclature
Through out this readme and API documentation (https://docs.rs/lamellar/latest/lamellar/) there are a few terms we end up reusing a lot, those terms and brief descriptions are provided below:
- `PE` - a processing element, typically a multi threaded process, for those familiar with MPI, it corresponds to a Rank.
    - Commonly you will create 1 PE per psychical CPU socket on your system, but it is just as valid to have multiple PE's per CPU
    - There may be some instances where `Node` (meaning a compute node) is used instead of `PE` in these cases they are interchangeable
- `World` - an abstraction representing your distributed computing system
    - consists of N PEs all capable of communicating with one another
- `Team` - A subset of the PEs that exist in the world
- `AM` - short for [Active Message](https://docs.rs/lamellar/latest/lamellar/active_messaging)
- `Collective Operation` - Generally means that all PEs (associated with a given distributed object) must explicitly participate in the operation, otherwise deadlock will occur.
    - e.g. barriers, construction of new distributed objects
- `One-sided Operation` - Generally means that only the calling PE is required for the operation to successfully complete.
    - e.g. accessing local data, waiting for local work to complete

# Features

Lamellar provides several different communication patterns and programming models to distributed applications, briefly highlighted below
## Active Messages
Lamellar allows for sending and executing user defined active messages on remote PEs in a distributed environment.
User first implement runtime exported trait (LamellarAM) for their data structures and then call a procedural macro [#\[lamellar::am\]](https://docs.rs/lamellar/latest/lamellar/active_messaging/attr.am.html) on the implementation.
The procedural macro produces all the necessary code to enable remote execution of the active message.
More details can be found in the [Active Messaging](https://docs.rs/lamellar/latest/lamellar/active_messaging) module documentation.

## Darcs (Distributed Arcs)
Lamellar provides a distributed extension of an [`Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) called a [Darc](https://docs.rs/lamellar/latest/lamellar/darc).
Darcs provide safe shared access to inner objects in a distributed environment, ensuring lifetimes and read/write accesses are enforced properly.
More details can be found in the [Darc](https://docs.rs/lamellar/latest/lamellar/darc) module documentation.

## PGAS abstractions

Lamellar also provides PGAS capabilities through multiple interfaces.

### LamellarArrays (Distributed Arrays)

The first is a high-level abstraction of distributed arrays, allowing for distributed iteration and data parallel processing of elements.
More details can be found in the [LamellarArray](https://docs.rs/lamellar/latest/lamellar/array) module documentation.

### Low-level Memory Regions

The second is a low level (unsafe) interface for constructing memory regions which are readable and writable from remote PEs.
Note that unless you are very comfortable/confident in low level distributed memory (and even then) it is highly recommended you use the LamellarArrays interface
More details can be found in the [Memory Region](https://docs.rs/lamellar/latest/lamellar/memregion) module documentation.

# Network Backends

Lamellar relies on network providers called Lamellae to perform the transfer of data throughout the system.
Currently the following Lamellae exist:
- `local` -  used for single-PE (single system, single process) development (this is the default),
- `shmem` -  used for multi-PE (single system, multi-process) development, useful for emulating distributed environments (communicates through shared memory)
- `rofi_c` - used for multi-PE (multi system, multi-process) distributed development, based on the Rust OpenFabrics Interface Transport Layer (ROFI) (<https://github.com/pnnl/rofi>).
    - Enabled by adding ```features = ["enable-rofi-c"]``` (or `enable-rofi-c-shared` / `enable-rofi-rust`) to the lamellar entry in your `Cargo.toml` file. This builds against ROFI's default PMI-1 backend; add `enable-rofi-c-pmix` (or `enable-rofi-c-shared-pmix`) instead to use ROFI's PMIx backend (ROFI has no PMI-2 client, so PMI-1/PMIx are the only choices).
- `libfabric` / `libfabric-sys` / `libfabric-async` - used for multi-PE distributed development directly against libfabric.
    - Enabled by adding ```features = ["enable-libfabric"]``` (see Cargo.toml for the `-sys`/`-async` variants) to the lamellar entry in your `Cargo.toml` file
- `ucx` - used for multi-PE distributed development based on [UCX](https://github.com/openucx/ucx).
    - Enabled by adding ```features = ["enable-ucx"]``` to the lamellar entry in your `Cargo.toml` file
    - All of the above distributed backends rely on system libraries (e.g. libfabric, ROFI, UCX) which may not be installed on your system.

The long term goal for lamellar is that you can develop using the `local` backend and then when you are ready to run distributed switch to a distributed backend (e.g. `rofi_c`, `libfabric`, `ucx`) with no changes to your code.
Currently the inverse is true, if it compiles and runs using a distributed backend it will compile and run when using `local` and `shmem` with no changes.

Additional information on using each of the lamellae backends can be found below in the `Running Lamellar Applications` section

## Vendored Dependencies

The distributed backends above depend on native C libraries (PMIx, PRRTE, libfabric, UCX, UCC, ROFI, hwloc, libevent). For each of these, lamellar can either build a bundled ("vendored") copy from source, or link against a system-installed version.

For the full breakdown of required packages, backend-by-backend native library tables, and system-install env vars, see [NATIVE_DEPENDENCIES.md](https://github.com/pnnl/lamellar/blob/master/NATIVE_DEPENDENCIES.md) in the parent `lamellar` repo (also mirrored at `NATIVE_DEPENDENCIES.md` in this directory).

**By default on Linux, lamellar vendors PMIx/PRRTE/libevent from source** — every native dependency pulled in by the default feature set (`with-pmix-vendored`, `enable-lamellar-main`, `enable-numa-detect`) is built from source except hwloc, which is auto-detected via system pkg-config by default. This is what you want on clusters/containers where you don't control what's installed, or where installed versions are outdated/incompatible. If no system hwloc/pkg-config is available (e.g. an offline build node), enable `vendored-hwloc` explicitly (see below). On macOS, hwloc's vendored autotools build is known to fail, so `vendored-hwloc` should stay disabled there and a system hwloc relied on instead.

The relevant feature flags:
- `with-pmix` (**enabled by default**) - selects PMIx as the backend for the `pmi` abstraction crate (used by `enable-libfabric`/`enable-ucx`), via `pmi/with-pmix`. Just a backend selection — doesn't vendor anything by itself, see `vendored-pmi` below.
- `vendored-pmi` (**enabled by default**) - builds `pmi`'s PMIx (and PMI/PMI2, if those backends are selected instead/as well) from source, via `pmi/vendored`. Without it, `pmi`'s backend links a system install.
- `with-pmix-vendored` - convenience alias equal to `with-pmix` + `vendored-pmi` together (matches `pmi`'s own feature name).
- `enable-lamellar-main` (**enabled by default**) - builds PRRTE from source (via `prrte-sys/prrte-src`) for the `#[lamellar::main]` launcher, and pulls in `pmix-sys` as PRRTE's PMIx dependency. hwloc is *not* included here — see `vendored-hwloc` below.
- `vendored-pmix` (**enabled by default**) - builds PMIx from source (via `pmix-sys/openpmix-src`) for whichever of `enable-lamellar-main` / `enable-rofi-c(-shared)` pulled `pmix-sys` in. Without it, a system PMIx is linked instead — useful on clusters where PMIx is already installed and version-matched to the system Slurm/PRRTE. **Note:** `pmix-sys` is a single shared crate — `vendored-pmi` above requests vendoring for it too (independently, for the `pmi`-crate use case), so both toggles need to be off to get a fully system-linked PMIx.
- `vendored-libevent` (**enabled by default**) - builds libevent from source for PRRTE (and, when PMIx is also vendored, for PMIx too), via `prrte-sys/vendored-libevent` and `pmix-sys/vendored-libevent`. Without it, a system libevent is linked instead.
- `vendored-hwloc` (**not enabled by default**) - builds hwloc from source, both for PRRTE (via `prrte-sys/vendored-hwloc`) and for lamellar's own NUMA-domain detection (via `hwlocality/vendored`). Without it, a system hwloc is auto-detected via pkg-config (or pointed to explicitly via `HWLOC_DIR`) — this is the default and the required setting on macOS, where hwloc's vendored autotools build is known to fail. Enable it only if no system hwloc/pkg-config is available.
- `enable-numa-detect` (**enabled by default**) - enables NUMA-domain detection (via `hwlocality`) used by `#[lamellar::main]` to pick PE-per-node defaults.
- `enable-rofi-c` / `enable-rofi-c-shared` - build ROFI against its default PMI-1 backend; no `pmix-sys` involved. Add `enable-rofi-c-pmix` / `enable-rofi-c-shared-pmix` on top to switch ROFI to its PMIx backend instead — pulls in `pmix-sys`, with vendoring controlled by the same `vendored-pmix`/`vendored-libevent` toggles above.
- `system-pmix-launcher` - convenience alias for the default feature set minus `vendored-pmi`/`vendored-pmix`/`vendored-libevent`, for clusters with a system-installed, version-matched PMIx/PRRTE (and libevent) — see the recipe below.

To link against system libraries instead, disable default features and select only what you need, e.g.:
```toml
lamellar = { version = "...", default-features = false, features = ["enable-rofi-c"] }
```

### Using a system install per dependency

Each native dependency has its own env-var override, checked by the corresponding `-sys`/`-src` crate's build script. Set the var and drop the matching vendoring feature; everything else about the feature set stays the same:

| Dependency | Env var(s) | Notes |
|---|---|---|
| PMIx | `PMIX_LIB_DIR` + `PMIX_INCLUDE_DIR` (links `pmix-sys` itself) and/or `PMIX_DIR` (PRRTE's own `--with-pmix`, if not pkg-config-discoverable) | drop `vendored-pmix`, and `vendored-pmi` too if `enable-libfabric`/`enable-ucx` are in use — see note above |
| PRRTE | `PRRTE_LIB_DIR` + `PRRTE_INCLUDE_DIR` + `PRRTE_BIN_DIR` | use `prrte-sys` with `prrte-src` omitted, see `prrte-sys`'s README |
| hwloc | `PKG_CONFIG_PATH` (e.g. `PKG_CONFIG_PATH=$HWLOC_DIR/lib/pkgconfig`) for `hwlocality` (pkg-config only, ignores `HWLOC_DIR`), **and** `HWLOC_DIR` for PRRTE's own vendored build — both needed together | not vendored by default already — only relevant if you turned on `vendored-hwloc` and want to turn it back off, or just need to point a non-standard hwloc install at pkg-config. **Required on macOS** since vendored hwloc's autotools build fails there |
| libevent | `LIBEVENT_DIR` (install prefix) | drop `vendored-libevent` |
| PMI/PMI2 | `PMI_LIB_DIR` / `PMI2_LIB_DIR` | drop `vendored-pmi` |
| libfabric | `OFI_DIR` (install prefix) | used automatically if set, no feature to disable |
| UCX | `UCX_DIR` (install prefix) | used automatically if set, no feature to disable |
| UCC | `UCC_DIR` (install prefix) | used automatically if set, no feature to disable |
| ROFI | `ROFI_DIR` (install prefix) | used automatically if set, no feature to disable |

Example, on an HPC cluster with a system-installed, version-matched PMIx/PRRTE/libevent already on `PKG_CONFIG_PATH` (or pointed to via `PMIX_DIR`/`LIBEVENT_DIR`) — hwloc still auto-detected via pkg-config as usual:
```toml
lamellar = { version = "...", default-features = false, features = ["system-pmix-launcher"] }
```

Example, PRRTE/PMIx/libevent vendored but hwloc linked from a system install (the macOS case) — this is just the default feature set (hwloc isn't vendored by default, so no feature overrides needed), pointed at a Homebrew hwloc:
```
HWLOC_DIR=$(brew --prefix hwloc) PKG_CONFIG_PATH=$(brew --prefix hwloc)/lib/pkgconfig cargo build
```
Requires `brew install hwloc libevent pkgconf`. If PMIx/PRRTE's `autoreconf` step fails
with `bad interpreter: /usr/bin/perl5.30: No such file or directory`, your Homebrew
autotools are linked against a stale perl — fix with `brew reinstall autoconf automake libtool`.

See each `-sys`/`-src` crate's README (`pmix-sys`, `prrte-sys`, `openpmix-src`, `prrte-src`, `lamellar-ucx-sys`, `lamellar-ucc-sys`, `rofi-sys`) for further detail on that crate's own vendoring sub-features.

# Using Lamellar 
Lamellar is capable of running on single node workstations as well as distributed HPC systems.
For a workstation, simply copy the following to the dependency section of you Cargo.toml file:

``` lamellar = "0.8.0" ```

If planning to use within a distributed HPC system copy the following to your Cargo.toml file:

```lamellar = { version = "0.8.0", features = ["enable-libfabric"]}``` # you can also use "enable-ucx", "enable-rofi-c" instead/in addition too

NOTE: as of Lamellar 0.6.1 It is no longer necessary to manually install Libfabric, the build process will now try to automatically build libfabric for you.
If this process fails, it is still possible to pass in a manual libfabric installation via the OFI_DIR envrionment variable.

For both environments, build your application as normal

```cargo build (--release)```

## Release Profiles
Lamellar's `Cargo.toml` defines three release profiles, tuned for different stages of development:

- `release` — fully optimized (`opt-level = 3`, `lto = true`, `codegen-units = 1`). Slowest to compile, best runtime performance. Use this for benchmarking and production runs.
  ```toml
  [profile.release]
  opt-level = 3
  lto = true
  codegen-units = 1
  ```
- `release-dev` — same optimization level as `release` but with LTO disabled and parallel codegen units, trading a bit of runtime performance for much faster incremental builds. We recommend copying this profile into your own application's `Cargo.toml` and using it during active development.
  ```toml
  [profile.release-dev]
  inherits = "release"
  opt-level = 3
  lto = false
  codegen-units = 256
  ```
- `release-debug` — `release-dev` plus debug symbols (including for dependencies), for when you need a debugger or backtraces on an otherwise optimized build.
  ```toml
  [profile.release-debug]
  inherits = "release-dev"
  debug = true
  [profile.release-debug.package."*"]
  debug = true
  ```

To build/run with a custom profile:
```
cargo build --profile release-dev --example <example-name>
cargo run --profile release-dev --example <example-name> -- <app args>
```

We do **not** recommend using the default `debug` profile (i.e. building without `--release`/`--profile`) for anything beyond quick correctness checks — the performance impact of unoptimized builds can significantly affect Lamellar's runtime execution, especially for communication-heavy applications.

# Running Lamellar Applications
A Lamellar application's `main` function must be annotated with `#[lamellar::main]` (see examples above), which is responsible for launching the requested number of PEs. You still construct the world yourself (e.g. via `LamellarWorldBuilder`) inside `main`.

The generated `main` detects whether it's already running as a launched PE (e.g. under PRRTE/srun); if not, it re-executes itself under a launcher (`prterun` by default, or `srun` with the `use-srun` feature) using the arguments given after the launcher-args separator described below. This means a single `cargo run`/binary invocation transparently becomes a multi-PE job — you no long need to invoke launcher commands manually

Before launching, `#[lamellar::main]` also patches the binary's `RPATH`/`RUNPATH` (via `readelf`/`patchelf`) with the build-output library directories found on `LD_LIBRARY_PATH`, so launched PEs can find dependencies like libfabric/UCX/rofi without you having to export `LD_LIBRARY_PATH` yourself. If this patching fails for some reason (e.g. `patchelf`/`readelf` unavailable), fall back to sourcing the generated `lamellar_env.sh` (built by `build.rs`) before running:
```
source lamellar_env.sh
```

## local / shmem (single system, one or many processes)
Launch directly, no separate launcher required:
```cargo run --release-dev --example <example-name> -- <app args>```

To run multiple PEs on a single system (shared-memory backend), add a second `--`-separated section with launcher args:
```
cargo run --profile release-dev --features '<feature-list>' --example <example-name> -- <app args> -- --pes 4 --lamellae shmem
```
- everything before the first `--` is `cargo`'s own arguments (profile, features, which binary/example to build)
- everything between the two `--` is forwarded to your application unmodified
- everything after the second `--` is forwarded to the launcher (`prterun`/`srun`) — `--np <N>` sets the number of PEs, `--map-by` controls process/thread placement, etc.

Example (uses `enable-ucx`, `enable-libfabric`, `enable-rofi-c`, and `tokio-executor` features together, launching 2 PEs, 4 threads each):
```
cargo run --profile release --features 'enable-ucx,enable-libfabric,enable-rofi-c,tokio-executor' --example load_store_test -- AtomicArray Block f32 128 -- --pes 2 --threads-per-pe 4 --lamellae shmem
```

or for an example where the binary does not take arguments (note the `-- --` this is necessary due to how cargo parses args):
```
cargo run --profile release --features 'enable-ucx,enable-libfabric,enable-rofi-c,tokio-executor' --example am_no_return -- -- --pes 2 --threads-per-pe 4 --lamellae shmem
```


**NOTE** passing these feature flags via command line are relevant to running the examples in this repository, in your own crate you would pass these directly to the lamellar entry in your cargo.toml file.

### Consistent PE flags (`--pes` / `--pes-per-node`)

Instead of hand-writing launcher-native flags (`--map-by node:PE=<N> --np <N>` for prterun, `--ntasks-per-node=<N> --cpus-per-task=<N>` for srun), you can use launcher-agnostic flags after the second `--`, and `#[lamellar::main]` translates them to whichever launcher is active (`prterun` or `srun`):
- `--pes <N>` — total number of PEs across all nodes
- `--pes-per-node <N>` — PEs per node
- `--threads-per-pe <N>` - Threads per pe

Give any or all (`LAMELLAR_PES`/`LAMELLAR_PES_PER_NODE,LAMELLAR_THREADS` env vars work as fallbacks too). If neither `--pes` or `--pes-per-node` is given (and `--nodes` wasn't either): with the `local` backend (the default when no other backend feature is enabled, or set explicitly via `LAMELLAR_BACKEND=local` or `--lamellae shmem`), defaults to a single PE using all available threads; every other backend (e.g. `shmem`) defaults to one PE per NUMA domain on the host. Cores bound per PE come from `--threads-per-pe <N>` (or `LAMELLAR_THREADS` if the flag isn't given); if neither is set, it defaults to `available cores / --pes-per-node` (or just available cores, if `--pes-per-node` wasn't given). For prterun, this is injected as `--map-by node:PE=<N>` — PRRTE's `PE=<N>` already binds each rank to N cpus, so on a host with more than one NUMA domain (detected via the `hwlocality`/hwloc bindings) NUMA-awareness comes from sizing `<N>` correctly rather than an explicit `--bind-to` (PRRTE rejects combining `PE=` with any `--bind-to` other than `core`/`hwt`). If `--pes-per-node` is smaller than the NUMA domain count, a single PE's cores would have to span more than one domain/package, which PRRTE always refuses — in that case `PE=` is dropped entirely and `--map-by node` is used instead, letting the rank float across the whole node. For srun, `--cpu-bind=ldoms` is added when multiple NUMA domains are detected. If you already pass a native flag (`--np`, `--map-by`, `--ntasks[-per-node]`, `--cpus-per-task`, `--cpu-bind`) yourself, it's left alone and nothing is injected on top of it.

```
cargo run --profile release-dev --example load_store_test -- AtomicArray Block f32 128 -- --pes 8 --pes-per-node 4 --threads-per-pe 4 --lamellae shmem
```

### Other launch flags

- `--lamellae <name>` — sets `LAMELLAR_BACKEND` for the launched job (same values as the `LAMELLAR_BACKEND` env var: `local`, `shmem`, `rofi_c`, `libfabric`, `libfabric-sys`, `libfabric-async`, `ucx`). `--help` lists which of these are actually available in the build (backend features that weren't enabled at compile time aren't offered) and which one is the compiled-in default.
- `--cmd-queue <variant>` — sets `LAMELLAR_CMD_QUEUE` for the launched job (`batched`, `get` (default), `geteager`, `getslots`, `put`, `putslots`, `puteager`).
- `--batcher <variant>` — sets `LAMELLAR_BATCHER` for the launched job (`simple` (default), `direct`, `team_am`, `vec_simple` (experimental), `vec_team_am` (experimental)).
- `--help` / `-h` — print all launch flags recognized after the second `--` and exit, without launching anything.

## distributed backends (multi-process, multi-system)
1. Allocate compute nodes on the cluster (this allows us to reuse nodes for subsequent runs without paying an allocation overhead):
    - ```salloc -N 2```
2. run your application the same way as above (`cargo run ... -- <app args> -- <launcher args>`) — `#[lamellar::main]` handles invoking `prterun`/`srun` across the allocated nodes; select the distributed backend via `--lamellae <backend>` or setting `LAMELLAR_BACKEND` (e.g. `rofi_c`, `libfabric`, `ucx`).

### Auto-allocation (`--nodes`, requires the `with-salloc` feature)

Step 1 above can be skipped: pass `--nodes <N>` after the second `--` (or set `LAMELLAR_NODES`) and, if you're not already inside a SLURM allocation, `#[lamellar::main]` runs `salloc -N <N> <your original command>` for you and blocks until it's granted, then proceeds as normal inside the allocation. If `--pes`/`--pes-per-node` is given without `--nodes`, the node count is derived from it (`nodes = ceil(pes / pes_per_node)`); giving all three requires them to agree (`pes == nodes * pes_per_node`) or the run fails fast with an error instead of guessing. If `salloc` isn't found, or no node count can be resolved, this step is skipped and behavior is unchanged from before.

This behavior is opt-in: build with `--features with-salloc` (in addition to `enable-lamellar-main`). Without it, `--nodes`/`LAMELLAR_NODES` is still parsed and still feeds PE-count defaults, but no `salloc` call is made — a warning is printed and the run proceeds as if `--nodes` weren't given.

Extra `salloc` flags (partition, time limit, account, qos, ...) can be passed through with `--salloc-opts`: everything after it, up to the next `--` (or end of args), is forwarded to `salloc` verbatim.

```
cargo run --release --example load_store_test -- AtomicArray Block f32 128 -- --nodes 2 --pes-per-node 4

cargo run --release --example load_store_test -- AtomicArray Block f32 128 -- --nodes 2 --salloc-opts --partition foo --time 01:00:00 --
```

Examples 
--------
All of the examples in the [documentation](https://docs.rs/lamellar/latest/lamellar) should also be valid Lamellar programs (please open an issue if you encounter an issue).

Our repository also provides numerous examples highlighting various features of the runtime: <https://github.com/pnnl/lamellar-runtime/tree/master/examples>

Additionally, we are compiling a set of benchmarks (some with multiple implementations) that may be helpful to look at as well: <https://github.com/pnnl/lamellar-benchmarks/>

Below are a few small examples highlighting some of the features of lamellar, more in-depth examples can be found in the documentation for the various features.
# Selecting a Lamellae and constructing a lamellar world instance
You can select which backend/lamellae to use by specifing the --lamellae <backend> arg to a binary (your binaries main function must be prepended with `#[lamellar::main]`)
```cargo run --release -- <app args> -- --lamellae <backend>```

or by setting the following envrionment variable:
```LAMELLAR_BACKEND="backend"``` where backend is one of `local`, `shmem`, `rofi_c`, `libfabric-sys`, `libfabric`, `libfabric-async`, or `ucx`.

or by setting the backend programmatically:
```
use lamellar::Backend;
#[lamellar::main]
fn main(){
 let mut world = lamellar::LamellarWorldBuilder::new()
        .with_lamellae( Default::default() ) //default is `Local` unless a distributed backend feature is active
        //.with_lamellae( Backend::RofiC ) //explicity set the lamellae backend to rofi_c,
        //.with_lamellae( Backend::Local ) //explicity set the lamellae backend to local
        //.with_lamellae( Backend::Shmem ) //explicity set the lamellae backend to use shared memory
        .build();
}
```


# Creating and executing a Registered Active Message
Please refer to the [Active Messaging](https://docs.rs/lamellar/latest/lamellar/active_messaging) documentation for more details and examples
```
use lamellar::active_messaging::prelude::*;

#[AmData(Debug, Clone)] // `AmData` is a macro used in place of `derive` 
struct HelloWorld { //the "input data" we are sending with our active message
    my_pe: usize, // "pe" is processing element == a node
}

#[lamellar::am] // at a highlevel registers this LamellarAM implemenatation with the runtime for remote execution
impl LamellarAM for HelloWorld {
    async fn exec(&self) {
        println!(
            "Hello pe {:?} of {:?}, I'm pe {:?}",
            lamellar::current_pe, 
            lamellar::num_pes,
            self.my_pe
        );
    }
}

#[lamellar::main] // launches the requested PEs (via prterun/srun) for you, see "Running Lamellar Applications" below; you still build the world yourself
fn main(){
    let mut world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let am = HelloWorld { my_pe: my_pe };
    for pe in 0..num_pes{
        world.exec_am_pe(pe,am.clone()).spawn(); // explicitly launch on each PE
    }
    world.wait_all(); // wait for all active messages to finish
    world.barrier();  // synchronize with other PEs
    let request = world.exec_am_all(am.clone()); //also possible to execute on every PE with a single call
    request.block(); //both exec_am_all and exec_am_pe return futures that can be used to wait for completion and access any returned result
}
```

# Creating, initializing, and iterating through a distributed array
Please refer to the [LamellarArray](https://docs.rs/lamellar/latest/lamellar/array) documentation for more details and examples
```
use lamellar::array::prelude::*;

#[lamellar::main]
fn main(){
    let world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let block_array = AtomicArray::<usize>::new(&world, 1000, Distribution::Block).block(); //we also support Cyclic distribution.
    block_array.dist_iter_mut().enumerate().for_each(move |(i,elem)| elem.store(i) ).block(); //simultaneosuly initialize array accross all pes, each pe only updates its local data
    block_array.barrier();
    if my_pe == 0{
        for (i,elem) in block_array.onesided_iter().into_iter().enumerate(){ //iterate through entire array on pe 0 (automatically transfering remote data)
            println!("i: {} = {})",i,elem);
        }
    }
}
```

# Utilizing a Darc within an active message
Please refer to the [Darc](https://docs.rs/lamellar/latest/lamellar/darc) documentation for more details and examples
```
use lamellar::active_messaging::prelude::*;
use std::sync::atomic::{AtomicUsize,Ordering};

#[AmData(Debug, Clone)] // `AmData` is a macro used in place of `derive` 
struct DarcAm { //the "input data" we are sending with our active message
    cnt: Darc<AtomicUsize>, // count how many times each PE executes an active message
}

#[lamellar::am] // at a highlevel registers this LamellarAM implemenatation with the runtime for remote execution
impl LamellarAM for DarcAm {
    async fn exec(&self) {
        self.cnt.fetch_add(1,Ordering::SeqCst);
    }
}

#[lamellar::main]
fn main(){
    let mut world = lamellar::LamellarWorldBuilder::new().build();
    let my_pe = world.my_pe();
    let num_pes = world.num_pes();
    let cnt = Darc::new(&world, AtomicUsize::new(0)).block().expect("calling pe is in the world");
    for pe in 0..num_pes{
        world.exec_am_pe(pe,DarcAm{cnt: cnt.clone()}).spawn(); // explicitly launch on each PE
    }
    world.exec_am_all(DarcAm{cnt: cnt.clone()}).spawn(); //also possible to execute on every PE with a single call
    cnt.fetch_add(1,Ordering::SeqCst); //this is valid as well!
    world.wait_all(); // wait for all active messages to finish
    world.barrier();  // synchronize with other PEs
    assert_eq!(cnt.load(Ordering::SeqCst),num_pes*2 + 1);
}
```

# Environment Variables

Please see [env_var.rs] for a description of available environment variables.

Commonly used variables include:
 - `LAMELLAR_THREADS` - The number of worker threads used within a lamellar PE, defaults to [std::thread::available_parallelism] if available or else 4
 - `LAMELLAR_BACKEND` - the backend used during execution. Note that if a backend is explicitly set in the world builder, this variable is ignored.
     - possible values
         - `local` -- default
         - `shmem`
         - `rofi_c`  -- only available with the `enable-rofi-c` (or related `enable-rofi-*`) feature
         - `libfabric-sys` / `libfabric` / `libfabric-async` -- only available with the corresponding `enable-libfabric*` feature
         - `ucx` -- only available with the `enable-ucx` feature
 - `LAMELLAR_EXECUTOR` - the executor used during execution. Note that if a executor is explicitly set in the world builder, this variable is ignored.
     - possible values
         - `lamellar` -- default, work stealing backend
         - `async_std` -- alternative backend from async_std
         - `tokio` -- only available with the `tokio-executor` feature in which case it is the default executor

Repository Organization 
-----------------------

Generally the 'master' branch corresponds to the latest stable release at [https://crates.io/crates/lamellar] and [https://docs.rs/lamellar/latest/lamellar/].
The 'dev' branch will contain the most recent 'working' features, where working means all the examples compile and execute properly (but the documentation may not yet be up-to-date).
All other branches are active feature branches and may or may not be in a working state.

NEWS
----
* July 2026: Alpha release -- v0.8.0
* November 2024: Alpha release -- v0.7.1
* November 2023: Alpha release -- v0.6.1
* February 2023: Alpha release -- v0.6
* January 2023: Alpha release -- v0.5
* March 2022: Alpha release -- v0.4
* April 2021: Alpha release -- v0.3
* September 2020: Add support for "local" lamellae, prep for crates.io release -- v0.2.1
* July 2020: Second alpha release -- v0.2
* Feb 2020: First alpha release -- v0.1

BUILD REQUIREMENTS
------------------


* Crates listed in Cargo.toml


Optional:
Lamellar requires the following dependencies if wanting to run in a distributed HPC environment:
the rofi lamellae is enabled by adding "enable-rofi-c" (or "enable-rofi-c-shared"/"enable-rofi-rust") to features either in cargo.toml or the command line when building. i.e. cargo build --features enable-rofi-c
Rofi can either be built from source and then setting the ROFI_DIR environment variable to the Rofi install directory, or by letting the rofi-sys crate build it automatically. Similarly, the `enable-ucx`/`enable-libfabric*` features enable the UCX/libfabric backends, each with their own `-sys` crate that can build its dependency automatically.

* [libfabric](https://github.com/ofiwg/libfabric) 
* [UCX](https://github.com/openucx/ucx)
* [ROFI](https://github.com/pnnl/rofi)
* [rofi-sys](https://github.com/pnnl/rofi-sys) -- available in [crates.io](https://crates.io/crates/rofisys)


At the time of release, Lamellar has been tested with the following external packages:

| **GCC** | **CLANG** | **ROFI**  | **OFI**       | **IB VERBS**  | **MPI**       | **SLURM** |
|--------:|----------:|----------:|--------------:|--------------:|--------------:|----------:|
| 7.1.0   | 8.0.1     | 0.1.0     | 1.20          | 1.13          | mvapich2/2.3a | 17.02.7   |


BUILDING PACKAGE
----------------
In the following, assume a root directory ${ROOT}

0. download Lamellar to ${ROOT}/lamellar-runtime

 `cd ${ROOT} && git clone https://github.com/pnnl/lamellar-runtime`

1. Select Lamellae to use:
    * In Cargo.toml add "enable-rofi-c" (or "enable-ucx", "enable-libfabric") feature if wanting to use a distributed backend (or pass --features enable-rofi-c to your cargo build command), otherwise only support for local and shmem backends will be built.

2. Compile Lamellar lib and test executable (feature flags can be passed to command line instead of specifying in cargo.toml)

`cargo build (--release) (--features enable-rofi-c)`

    executables located at ./target/debug(release)/test

3. Compile Examples

`cargo build --examples (--release) (--features enable-rofi-c) `

    executables located at ./target/debug(release)/examples/

Note: we do an explicit build instead of `cargo run --examples` as they are intended to run in a distriubted envrionment (see TEST section below.)

HISTORY
-------
- version 0.8.0
  - new UCX lamellae backend (`enable-ucx`), including sub-team allocation, on-node fastpath, and UCC-based network-accelerated collectives
  - new libfabric lamellae backends (standard/`-sys`/`-async` variants) with network-accelerated collectives (allreduce, gather, scatter, alltoall, broadcast)
  - ROFI backend refactored into a `rofi_c` design with improved lifecycle/drop handling and atomic-op fixes
  - new `shmem`/`local` backends with PMI-based PE discovery and their own collective implementations
  - new `lamellar_main` launcher crate/macro for automated distributed session launch (srun/mpirun-style), with vendored PMIx/PRRTE
  - opt-in runtime statistics (`enable-stats`) and profiling (`enable-prof`) features, gating counters behind them to reduce overhead when disabled
  - network-accelerated atomics and `compare_exchange` support across all backends
  - blocking/async comm APIs unified through a shared `Scheduler`
  - reduced costly `Darc`/`Arc` clones in array iteration and request metadata paths for better hot-path performance
  - various optimizations and fixes
- version 0.7.0
  - add support for integration with various async executor backends including tokio and async-std
  - 'handle' based api, allowing for 'spawn()'ing, 'block()'ing, and 'await'ing remote operations.
  - conversion from `Pin<Box<dyn Future>>` to concrete types for most remote operations.
  - improved execution time warning framework for potential deadlock, unexecuted remote operations, blocking calls in async code, etc.
    - can be completely disabled
    - can panic instead of print warning
  - various optimizations and bug fixes
- version 0.6.1
  - Clean up apis for lock based data structures
  - N-way dissemination barrier
  - Fixes for AM visibility issues
  - Better error messages
  - Update Rofi lamellae to utilize rofi v0.3
  - Various fixes for Darcs
- version 0.6
  - LamellarArrays
    - additional iterator methods
      - count
      - sum
      - reduce
    - additional element-wise operations
      - remainder
      - xor
      - shl, shr
    - Backend operation batching improvements
    - variable sized array indices
    - initial implementation of GlobalLockArray
    - 'ArrayOps' trait for enabling user defined element types
  - AM Groups - Runtime provided aggregation of AMs
    - Generic 'AmGroup'
    - 'TypedAmGroup'
        - 'static' group members
  - Miscellaneous
    - added LAMELLLAR_DEADLOCK_TIMEOUT to help with stalled applications
    - better error handling and exiting on panic and critical failure detection
    - backend threading improvements
    - LamellarEnv trait for accessing various info about the current lamellar envrionment
    - additional examples
    - updated documentation
- version 0.5
  - Vastly improved documentation (i.e. it exists now ;))
  - 'Asyncified' the API - most remote operations now return Futures
  - LamellarArrays
    - Additional OneSidedIterators, LocalIterators, DistributedIterators
    - Additional element-wise operations
    - For Each "schedulers"
    - Backend optimizations
  - AM task groups
  - AM backend updates
  - Hooks for tracing
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
  - added an example proxy app for doing a distributed DFT
- version 0.2:
  - New user facing API
  - Registered Active Messages (enabling stable rust)
  - Remote Closures feature guarded for use with nightly rust
  - redesigned internal lamellae organization
  - initial support for world and teams (sub groups of PE)
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

Current Team Members

Ryan Friese           - ryan.friese@pnnl.gov  
Erdal Mutlu           - erdal.mutlu@pnnl.gov  
Joseph Cottam         - joseph.cottam@pnnl.gov
Greg Roek             - gregory.roek@pnnl.gov
Mark Raugas           - mark.raugas@pnnl.gov  

Past Team Members

Roberto Gioiosa       - roberto.gioiosa@pnnl.gov
Polykarpos Thomadakis - polykarpos.thomadakis@pnnl.gov

## License

This project is licensed under the BSD License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

This work was supported by the High Performance Data Analytics (HPDA) Program at Pacific Northwest National Laboratory (PNNL),
a multi-program DOE laboratory operated by Battelle.
