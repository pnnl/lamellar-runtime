use std::collections::HashSet;
use std::sync::Mutex;

static BUILT: Mutex<Option<HashSet<String>>> = Mutex::new(None);

/// Cargo builds this test binary at `target/<profile>/deps/<bin>`, and example
/// binaries at `target/<profile>/examples/<bin>` — both under the same profile
/// dir for a given `cargo test --profile <profile>` invocation. Walking up from
/// our own exe path recovers that profile name with no env var required.
pub fn profile() -> String {
    let exe = std::env::current_exe().expect("failed to get current exe path");
    exe.ancestors()
        .nth(2)
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("release")
        .to_string()
}

/// This crate's own `[features]` (Cargo.toml), kept in sync by hand — a test
/// binary is a target of this same package, so `cfg!(feature = "..")` here
/// reflects exactly the `--features`/`--no-default-features` the outer
/// `cargo test` was invoked with.
macro_rules! feature_list {
    ($($name:literal),* $(,)?) => {{
        let mut v = Vec::new();
        $(if cfg!(feature = $name) { v.push($name); })*
        v
    }};
}

fn active_features() -> Vec<&'static str> {
    feature_list![
        "enable-lamellar-main",
        "vendored-hwloc",
        "enable-numa-detect",
        "with-salloc",
        "enable-stats",
        "enable-rofi-c",
        "enable-rofi-c-shared",
        "rofi-c",
        "enable-rofi-rust",
        "enable-libfabric",
        "enable-libfabric-sys",
        "enable-libfabric-async",
        "enable-ucx",
        "tokio-executor",
        "disable-runtime-warnings",
        "runtime-warnings-panic",
        "slurm-test",
        "enable-on-node-shmem",
        "with-pmi1",
        "with-pmi2",
        "with-pmix",
        "with-pmix-vendored",
        "vendored-pmi",
        "enable-prof",
    ]
}

/// Builds the named example under the current profile, once per test-binary
/// process. `cargo test` never builds `[[example]]` targets on its own, so
/// tests that shell out to a prebuilt example binary need this first. The
/// example is built with exactly this test binary's own active feature set,
/// so backend/config choices (e.g. rofi vs libfabric) stay consistent.
pub fn ensure_example_built(name: &str) {
    {
        let mut guard = BUILT.lock().unwrap();
        let set = guard.get_or_insert_with(HashSet::new);
        if !set.insert(name.to_string()) {
            return;
        }
    }

    println!("building example `{name}` for profile `{}`", profile());
    let mut cmd = std::process::Command::new(env!("CARGO"));
    // --test <this test's own crate name> keeps this in the same dev-dependency
    // feature-unification graph that `cargo test` itself uses, without pulling
    // in every other test binary the way plain --tests would. Without it,
    // `cargo test` and this `cargo build --example` invocation resolve shared
    // deps' features differently, and alternating between the two thrashes the
    // target dir cache.
    cmd.arg("build")
        .arg("--example")
        .arg(name)
        .arg("--test")
        .arg(env!("CARGO_CRATE_NAME"));
    match profile().as_str() {
        "debug" => {}
        "release" => {
            cmd.arg("--release");
        }
        other => {
            cmd.arg("--profile").arg(other);
        }
    }

    cmd.arg("--no-default-features");
    let features = active_features();
    if !features.is_empty() {
        cmd.arg("--features").arg(features.join(","));
    }

    println!("running: {cmd:?}");

    let status = cmd
        .status()
        .unwrap_or_else(|e| panic!("failed to run cargo build for example `{name}`: {e}"));
    assert!(status.success(), "failed to build example `{name}`");
}
