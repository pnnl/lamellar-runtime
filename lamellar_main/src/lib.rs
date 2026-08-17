//! Lamellar-specific launcher wrapper around [`hpc_launch`], providing
//! `#[lamellar_main::main]` / `#[lamellar_main::test]`. Adds
//! `--lamellae`/`--cmd-queue`/`--batcher` launch flags, lamellar's backend
//! list in `--help`, backend-aware PE defaulting, and `enable-prof` support
//! on top of the generic launch support `hpc_launch` provides standalone.
//!
//! Most users get this for free via `lamellar`'s `enable-lamellar-main`
//! feature, which reexports [`main`]/[`test`] as `#[lamellar::main]`/
//! `#[lamellar::test]`.

pub use lamellar_main_impl::main;
pub use hpc_launch::test;

pub use hpc_launch::{
    core_count, cores_per_package, generic_help_text, init_tracing_from_env, numa_domain_count,
    package_count, pe_id, pu_count, resolve_pe_defaults,
};

#[cfg(feature = "use-prterun")]
pub use hpc_launch::prrte_sys;
