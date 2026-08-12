//! Support crate for `#[hpc_launch::main]` / `#[hpc_launch::test]`.
//!
//! This crate can be used standalone (without depending on the `lamellar`
//! runtime crate) to get a self-launching binary via `prterun`/`srun`. It
//! also reexports [`prrte_sys`] and provides best-effort hardware-topology
//! and tracing-init helpers used by the generated launch code.

pub use hpc_launch_impl::main;
pub use hpc_launch_impl::test;

#[cfg(feature = "use-prterun")]
pub use prrte_sys;

/// Initializes a `tracing-subscriber` global subscriber from the
/// `LAMELLAR_LOG` environment variable, if set. Mirrors `RUST_LOG` syntax.
/// A no-op if `LAMELLAR_LOG` is unset or empty, or if a global subscriber is
/// already installed.
pub fn init_tracing_from_env() {
    use tracing_subscriber::prelude::*;

    if let Ok(lamellar_log) = std::env::var("LAMELLAR_LOG") {
        if !lamellar_log.trim().is_empty() {
            std::env::set_var("RUST_LOG", &lamellar_log);
            let _ = tracing_subscriber::registry()
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_thread_ids(true)
                        .with_file(true)
                        .with_line_number(true)
                        .with_level(true),
                )
                .try_init();
        }
    }
}

/// Base `--help` body text (generic launch flags only, no header/footer) for
/// binaries built with `#[hpc_launch::main]`. Callers wrap this with their
/// own header line, `--help`/`-h` line, and trailing "passed through"
/// line, and may append their own additional flag lines after it.
pub fn generic_help_text() -> String {
    let mut s = String::new();
    s.push_str("  --nodes <n>            number of nodes to allocate/launch across [env: LAMELLAR_NODES]\n");
    s.push_str("  --pes <n>              total number of PEs to launch [env: LAMELLAR_PES]\n");
    s.push_str("  --pes-per-node <n>     PEs to launch per node [env: LAMELLAR_PES_PER_NODE]\n");
    s.push_str("  --threads-per-pe <n>   worker threads per PE [env: LAMELLAR_THREADS]\n");
    s.push_str("  --log <filter>         tracing filter for the launched job (RUST_LOG syntax) [env: LAMELLAR_LOG]\n");
    if cfg!(feature = "with-salloc") {
        s.push_str("  --salloc-opts <opts>   extra options passed to `salloc` when requesting an allocation\n");
    }
    s.push_str("  --output-dir <dir>     directory to write per-PE stdout/stderr into\n");
    s.push_str("  --time                 report wall-clock launch time\n");
    s.push_str("  --gdb [bt]             launch each PE under `rust-gdb` (with `bt`: auto backtrace on crash)\n");
    s
}

/// Resolves `--pes`/`--pes-per-node` defaults when neither was given on the
/// command line. If `nodes` is known, splits one PE per NUMA domain across
/// all nodes; otherwise, if `default_single_pe` is set, defaults to a single
/// PE; otherwise defaults to one PE per NUMA domain on the local host.
pub fn resolve_pe_defaults(
    nodes: Option<u32>,
    numa_domains: Option<u32>,
    default_single_pe: bool,
) -> (Option<u32>, Option<u32>) {
    if let Some(n) = nodes {
        let ppn = numa_domains.unwrap_or(1).max(1);
        (Some(n * ppn), Some(ppn))
    } else if default_single_pe {
        (Some(1), Some(1))
    } else {
        let ppn = numa_domains.unwrap_or(1).max(1);
        (Some(ppn), Some(ppn))
    }
}

/// Codegen for patching the launched binary's RPATH/RUNPATH so build-script
/// output shared libraries (found via `LD_LIBRARY_PATH`) are resolvable at
/// runtime, even after the binary has been re-invoked under
/// `prterun`/`srun`.
#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
pub fn binary_update_block_tokens() -> proc_macro2::TokenStream {
    quote::quote! {
        {
            let ld_library_path = std::env::var("LD_LIBRARY_PATH").unwrap_or_else(|_| String::new());
            let mut shared_libs_dirs = Vec::new();
            for p in ld_library_path.split(':') {
                // Collect all build output library directories matching the pattern .../target/<profile>/build/.../out/lib
                if p.contains("/target/") && p.contains("/build/") && p.contains("/out/lib") {
                    shared_libs_dirs.push(p.to_string());
                }
            }
            let shared_libs_dir = shared_libs_dirs.join(":");
            if !shared_libs_dir.is_empty() {
                if let Ok(exe_path) = std::env::current_exe() {
                    let readelf_out = std::process::Command::new("readelf")
                        .arg("-d")
                        .arg(&exe_path)
                        .output();
                    match readelf_out {
                        Ok(readelf_out) if readelf_out.status.success() => {
                            let readelf_stdout = String::from_utf8_lossy(&readelf_out.stdout);
                            let mut has_shared_libs_dir = false;
                            let mut existing_rpath = String::new();

                            for line in readelf_stdout.lines() {
                                if line.contains("(RPATH)") || line.contains("(RUNPATH)") {
                                    if let (Some(start), Some(end)) = (line.find('['), line.rfind(']')) {
                                        let cur = line[start + 1..end].trim();
                                        if !cur.is_empty() {
                                            existing_rpath = cur.to_string();
                                            has_shared_libs_dir = shared_libs_dir
                                                .split(':')
                                                .all(|lib_dir| existing_rpath.contains(lib_dir));
                                        }
                                    }
                                }
                            }

                            if !has_shared_libs_dir {
                                let new_rpath = if existing_rpath.is_empty() {
                                    shared_libs_dir.to_string()
                                } else {
                                    format!("{}:{}", existing_rpath, shared_libs_dir)
                                };

                                let exe_dir = exe_path.parent().unwrap_or(std::path::Path::new("."));
                                let temp_exe = exe_dir.join(format!("hpc_launch_exe_{}.tmp", std::process::id()));
                                let temp_exe_str = temp_exe.to_string_lossy().to_string();

                                if let Err(err) = std::fs::copy(&exe_path, &temp_exe) {
                                    eprintln!(
                                        "hpc_launch: failed to copy binary from {:?} to {}: {}",
                                        exe_path,
                                        temp_exe_str,
                                        err
                                    );
                                } else {
                                    let timer = std::time::Instant::now();
                                    while let Ok(false) = std::fs::exists(&temp_exe) {
                                        std::thread::sleep(std::time::Duration::from_millis(100));
                                        if timer.elapsed() > std::time::Duration::from_secs(1) {
                                            eprintln!(
                                                "hpc_launch: timeout waiting for temp binary {:?} to appear after copying: {}",
                                                temp_exe,
                                                temp_exe_str
                                            );
                                            break;
                                        }
                                    }
                                    let chmod_status = std::process::Command::new("chmod")
                                        .arg("+x")
                                        .arg(&temp_exe_str)
                                        .status();

                                    println!("chmod status: {:?}", chmod_status);

                                    let patch_status = std::process::Command::new("patchelf")
                                        .arg("--set-rpath")
                                        .arg(&new_rpath)
                                        .arg(&temp_exe_str)
                                        .status();

                                    match patch_status {
                                        Ok(status) if status.success() => {
                                            if let Err(err) = std::fs::rename(&temp_exe, &exe_path) {
                                                eprintln!(
                                                    "hpc_launch: failed to replace original binary {:?} with patched copy: {}",
                                                    exe_path,
                                                    err
                                                );
                                                let _ = std::fs::remove_file(&temp_exe);
                                            }
                                        }
                                        Ok(status) => {
                                            eprintln!(
                                                "hpc_launch: patchelf failed with status {} for {:?}",
                                                status,
                                                temp_exe_str
                                            );
                                            let _ = std::fs::remove_file(&temp_exe);
                                        }
                                        Err(err) => {
                                            eprintln!(
                                                "hpc_launch: failed to run patchelf for {:?}: {}",
                                                temp_exe_str,
                                                err
                                            );
                                            let _ = std::fs::remove_file(&temp_exe);
                                        }
                                    }
                                }
                            }
                        }
                        Ok(status_out) => {
                            eprintln!(
                                "hpc_launch: readelf -d failed for {:?} with status {}",
                                exe_path,
                                status_out.status
                            );
                        }
                        Err(err) => {
                            eprintln!(
                                "hpc_launch: failed to run readelf for {:?}: {}",
                                exe_path,
                                err
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Codegen for the `#[test]`-launched sibling function generated by
/// `#[hpc_launch::test]` — re-invokes the compiled test binary under
/// `prterun`/`srun`, filtered down to just the single launched test.
#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
pub fn launch_test_block_tokens(
    launcher_info: (impl quote::ToTokens, impl quote::ToTokens),
    test_attr: syn::Attribute,
    vis: &syn::Visibility,
    sig: &syn::Signature,
    name: &syn::Ident,
) -> proc_macro2::TokenStream {
    let (env_var, launcher_path) = launcher_info;
    quote::quote! {
        #test_attr #vis #sig {
        fn type_name_of_val<T: ?Sized>(_val: &T) -> &'static str {
            std::any::type_name::<T>()
        }

        let full_func_name = type_name_of_val(&#name);
        let func_name = full_func_name
            .split("::")
            .into_iter()
            .skip(1)
            .collect::<Vec<&str>>()
            .join("::");

        let prte_launched = std::env::var(#env_var).is_ok();
        if !prte_launched {
            let mut args: Vec<String> = std::env::args().collect();
            let exec = args.remove(0);
            let mut prterun_args = Vec::<String>::new();

            let pos = args.iter().position(|x| x == "--");
            if let Some(pos) = pos {
                args.split_off(pos).into_iter().skip(1).for_each(|x| {
                    prterun_args.push(x.to_string());
                });
            }
            let end = args.len();

            prterun_args.push(exec);
            prterun_args.push(func_name);

            prterun_args.extend(args.into_iter());
            prterun_args.push("--ignored".to_string());
            prterun_args.push("--exact".to_string());
            std::process::Command::new(#launcher_path)
                .args(prterun_args)
                .status()
                .expect("failed to launch process");
        }
        }
    }
}

/// Returns the current process's PE (rank) id, if known, without requiring
/// any `LamellarWorld`/team object to have been created first.
///
/// Reads `PMIX_RANK` (set on every launched child by PRRTE's vendored PMIx
/// server via `PMIx_server_setup_fork`, regardless of whether the process
/// goes on to call into PMIx itself), falling back to `PMI_RANK` (PMI1/PMI2
/// convention, set by launchers using those older protocols instead of
/// PMIx) and then `SLURM_PROCID` (`srun`-launched jobs). Returns `None` if
/// launched under none of these (e.g. running standalone, pre-launch, or
/// under an unrecognized launcher).
pub fn pe_id() -> Option<u32> {
    std::env::var("PMIX_RANK")
        .ok()
        .or_else(|| std::env::var("PMI_RANK").ok())
        .or_else(|| std::env::var("SLURM_PROCID").ok())
        .and_then(|s| s.parse().ok())
}

/// Best-effort count of NUMA domains on the current host. Uses `hwlocality`
/// when the `enable-numa-detect` feature is enabled, falling back to a
/// `/sys` scan (and to that same scan unconditionally when the feature is
/// disabled). Returns `None` if neither method can determine a count.
pub fn numa_domain_count() -> Option<u32> {
    #[cfg(feature = "enable-numa-detect")]
    {
        use hwlocality::object::types::ObjectType;
        use hwlocality::Topology;
        if let Some(count) = Topology::new()
            .ok()
            .map(|t| t.objects_with_type(ObjectType::NUMANode).count() as u32)
        {
            return Some(count);
        }
    }
    numa_domain_count_fs()
}

fn numa_domain_count_fs() -> Option<u32> {
    let mut count = 0u32;
    for entry in std::fs::read_dir("/sys/devices/system/node").ok()?.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let rest = match name.strip_prefix("node") {
            Some(r) => r,
            None => continue,
        };
        if !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()) {
            count += 1;
        }
    }
    if count == 0 { None } else { Some(count) }
}

/// Best-effort count of physical packages (sockets) on the current host.
/// Uses `hwlocality` when the `enable-numa-detect` feature is enabled,
/// falling back to a `/sys` scan.
pub fn package_count() -> Option<u32> {
    #[cfg(feature = "enable-numa-detect")]
    {
        use hwlocality::object::types::ObjectType;
        use hwlocality::Topology;
        if let Some(count) = Topology::new()
            .ok()
            .map(|t| t.objects_with_type(ObjectType::Package).count() as u32)
        {
            return Some(count);
        }
    }
    package_count_fs()
}

fn package_count_fs() -> Option<u32> {
    use std::collections::HashSet;
    let mut ids = HashSet::new();
    for entry in std::fs::read_dir("/sys/devices/system/cpu").ok()?.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let rest = match name.strip_prefix("cpu") {
            Some(r) => r,
            None => continue,
        };
        if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let path = entry.path().join("topology/physical_package_id");
        if let Ok(content) = std::fs::read_to_string(&path) {
            if let Ok(id) = content.trim().parse::<u32>() {
                ids.insert(id);
            }
        }
    }
    if ids.is_empty() { None } else { Some(ids.len() as u32) }
}

/// Best-effort count of physical cores on the current host (i.e. excluding
/// hyperthreads/SMT siblings). Uses `hwlocality` when the
/// `enable-numa-detect` feature is enabled, falling back to a `/sys` scan.
pub fn core_count() -> Option<u32> {
    #[cfg(feature = "enable-numa-detect")]
    {
        use hwlocality::object::types::ObjectType;
        use hwlocality::Topology;
        if let Some(count) = Topology::new()
            .ok()
            .map(|t| t.objects_with_type(ObjectType::Core).count() as u32)
        {
            return Some(count);
        }
    }
    core_count_fs()
}

fn core_count_fs() -> Option<u32> {
    use std::collections::HashSet;
    let mut ids = HashSet::new();
    for entry in std::fs::read_dir("/sys/devices/system/cpu").ok()?.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let rest = match name.strip_prefix("cpu") {
            Some(r) => r,
            None => continue,
        };
        if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let path = entry.path();
        let package_id = std::fs::read_to_string(path.join("topology/physical_package_id"))
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok());
        let core_id = std::fs::read_to_string(path.join("topology/core_id"))
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok());
        if let (Some(pkg), Some(core)) = (package_id, core_id) {
            ids.insert((pkg, core));
        }
    }
    if ids.is_empty() { None } else { Some(ids.len() as u32) }
}

/// Best-effort count of processing units (logical CPUs, including SMT
/// siblings) on the current host. Uses `hwlocality` when the
/// `enable-numa-detect` feature is enabled, falling back to a `/sys` scan.
pub fn pu_count() -> Option<u32> {
    #[cfg(feature = "enable-numa-detect")]
    {
        use hwlocality::object::types::ObjectType;
        use hwlocality::Topology;
        if let Some(count) = Topology::new()
            .ok()
            .map(|t| t.objects_with_type(ObjectType::PU).count() as u32)
        {
            return Some(count);
        }
    }
    pu_count_fs()
}

fn pu_count_fs() -> Option<u32> {
    let mut count = 0u32;
    for entry in std::fs::read_dir("/sys/devices/system/cpu").ok()?.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let rest = match name.strip_prefix("cpu") {
            Some(r) => r,
            None => continue,
        };
        if !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()) {
            count += 1;
        }
    }
    if count == 0 { None } else { Some(count) }
}
