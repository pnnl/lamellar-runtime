use proc_macro::TokenStream;
// use proc_macro2::TokenStream;
use quote::quote;
#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
use quote::ToTokens;
use syn::{parse_macro_input, parse_quote, Attribute, ItemFn};

#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
fn create_binary_update_block() -> impl ToTokens {
    quote! {
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
            // println!("shared_libs_dir: {}", shared_libs_dir);
            if !shared_libs_dir.is_empty() {
                if let Ok(exe_path) = std::env::current_exe() {
                    let readelf_out = std::process::Command::new("readelf")
                        .arg("-d")
                        .arg(&exe_path)
                        .output();
                    // println!("readelf output: {:?}", readelf_out);
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
                                            // Check if all required libs are already in RPATH
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
                                    // Combine existing RPATH with new library directories
                                    format!("{}:{}", existing_rpath, shared_libs_dir)
                                };

                                // Copy binary to temporary location since we can't patchelf a running binary
                                // Create temp file in same directory as the binary to avoid cross-filesystem rename issues
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
                                        // Wait for the copied file to be fully written to disk before trying to patch it
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
                                    // Make the copy executable
                                    let chmod_status = std::process::Command::new("chmod")
                                        .arg("+x")
                                        .arg(&temp_exe_str)
                                        .status();

                                    println!("chmod status: {:?}", chmod_status);

                                    let patch_status = std::process::Command::new("patchelf")
                                        // .arg("--force-runpath")
                                        .arg("--set-rpath")
                                        .arg(&new_rpath)
                                        .arg(&temp_exe_str)
                                        .status();

                                    match patch_status {
                                        Ok(status) if status.success() => {
                                            // println!(
                                            //     "updated RPATH for {:?} to {}",
                                            //     temp_exe_str,
                                            //     new_rpath
                                            // );
                                            // Replace the original binary with the patched copy
                                            if let Err(err) = std::fs::rename(&temp_exe, &exe_path) {
                                                eprintln!(
                                                    "hpc_launch: failed to replace original binary {:?} with patched copy: {}",
                                                    exe_path,
                                                    err
                                                );
                                                // Clean up temp file on failure
                                                let _ = std::fs::remove_file(&temp_exe);
                                            } else {
                                                // println!(
                                                //     "successfully replaced original binary with patched version"
                                                // );
                                            }
                                        }
                                        Ok(status) => {
                                            eprintln!(
                                                "hpc_launch: patchelf failed with status {} for {:?}",
                                                status,
                                                temp_exe_str
                                            );
                                            // Clean up temp file on failure
                                            let _ = std::fs::remove_file(&temp_exe);
                                        }
                                        Err(err) => {
                                            eprintln!(
                                                "hpc_launch: failed to run patchelf for {:?}: {}",
                                                temp_exe_str,
                                                err
                                            );
                                            // Clean up temp file on error
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

#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
fn create_launch_block(
    launcher_info: (impl ToTokens, impl ToTokens),
    ret: Option<impl ToTokens>,
    // "srun" or "prterun" to control output flag format
    launcher_type: &str,
) -> impl ToTokens {
    let (env_var, launcher_path) = launcher_info;

    let output_dir_block = if launcher_type == "srun" {
        quote! {
            if !output_dir.is_empty() {
                let __dir = output_dir.trim_end_matches('/');
                std::fs::create_dir_all(__dir)
                    .expect("hpc_launch: failed to create output directory");
                // srun uses %t to expand to the task ID
                prterun_args.insert(0, format!("--error={}/pe_%t.err", __dir));
                prterun_args.insert(0, format!("--output={}/pe_%t.out", __dir));
            }
        }
    } else {
        // prterun (or other launcher using prterun-style output syntax)
        quote! {
            if !output_dir.is_empty() {
                let __dir = output_dir.trim_end_matches('/');
                std::fs::create_dir_all(__dir)
                    .expect("hpc_launch: failed to create output directory");
                // PRRTE dir= mode can fail with PMIX_ERR_EXISTS in some bundled builds.
                // Use file= mode instead and keep outputs grouped under the requested dir.
                prterun_args.insert(
                    0,
                    format!("--merge-stderr-to-stdout"),
                );
                prterun_args.insert(
                    0,
                    format!("--output=file={}/hpc_launch", __dir),
                );
            }
        }
    };
    // dbg!("env var: {:?}", std::env::vars());

    let pe_injection_block = if launcher_type == "srun" {
        quote! {
            if pes.is_some() || pes_per_node.is_some() {
                if let Some(ppn) = pes_per_node {
                    if !has_flag(&prterun_args, &["--ntasks-per-node"]) {
                        prterun_args.push(format!("--ntasks-per-node={}", ppn));
                    }
                } else if let Some(p) = pes {
                    if !has_flag(&prterun_args, &["--ntasks"]) {
                        prterun_args.push(format!("--ntasks={}", p));
                    }
                }
                if !has_flag(&prterun_args, &["--cpus-per-task"]) {
                    prterun_args.push(format!("--cpus-per-task={}", threads_per_pe));
                }
                if let Some(d) = numa_domains {
                    if d > 1 && !has_flag(&prterun_args, &["--cpu-bind"]) {
                        prterun_args.push("--cpu-bind=ldoms".to_string());
                    }
                }
            }
        }
    } else {
        // prterun (or other launcher using prterun-style flags)
        quote! {
            if pes.is_some() || pes_per_node.is_some() {
                if let Some(p) = pes {
                    if !has_flag(&prterun_args, &["--np", "-np"]) {
                        prterun_args.push("--np".to_string());
                        prterun_args.push(p.to_string());
                    }
                }
                if pes_per_node.is_some() && !has_flag(&prterun_args, &["--map-by"]) {
                    // Keep the mapping *policy* as "node" so ranks still spread across
                    // all allocated nodes. `PE=<n>` implicitly binds each rank to <n>
                    // cpus, and PRRTE rejects combining a PE= directive with any
                    // --bind-to other than "core"/"hwt" -- so NUMA-awareness here
                    // comes entirely from sizing PE= to threads_per_pe. But if a single
                    // PE needs more cores than fit in one package (socket), that
                    // per-core binding would have to span multiple packages, which
                    // PRRTE always refuses ("bound to CPUs in more than one package").
                    // In that case drop PE= entirely and let the rank float across the
                    // whole node. This is a package boundary, not a NUMA domain one --
                    // a package can contain multiple NUMA domains, so pes_per_node can
                    // be well below numa_domains while still fitting in one package.
                    // pes_per_node < packages is only a necessary precondition for that,
                    // not sufficient -- only actually span if threads_per_pe wouldn't
                    // fit within a single package's cores.
                    let spans_multiple_domains = match packages {
                        Some(p) if p > 1 && pes_per_node.unwrap_or(1) < p => {
                            match ::hpc_launch::cores_per_package() {
                                Some(cores_per_pkg) => threads_per_pe > cores_per_pkg,
                                None => true,
                            }
                        }
                        _ => false,
                    };
                    prterun_args.push("--map-by".to_string());
                    if spans_multiple_domains {
                        prterun_args.push("node".to_string());
                        // No PE= directive here, so PRRTE falls back to its own
                        // default binding policy (CORE:IF-SUPPORTED), pinning the
                        // rank to a single core -- which would make
                        // available_parallelism() (and thus the LAMELLAR_THREADS
                        // fallback) see only 1 core. Explicitly unbind instead so
                        // the rank can use every core it spans.
                        if !has_flag(&prterun_args, &["--bind-to"]) {
                            prterun_args.push("--bind-to".to_string());
                            prterun_args.push("none".to_string());
                        }
                    } else {
                        prterun_args.push(format!("node:PE={}", threads_per_pe));
                    }
                }
            }
        }
    };

    let salloc_block = if cfg!(feature = "with-salloc") {
        quote! {
            // If we're not already inside a SLURM allocation and a node count was
            // resolved, request one via `salloc` and re-invoke this same binary
            // (with its original argv) inside it.
            let in_allocation = std::env::var("SLURM_JOB_ID").is_ok();
            if !in_allocation {
                if let Some(n) = nodes {
                    let salloc_result = std::process::Command::new("salloc")
                        .arg("-N")
                        .arg(n.to_string())
                        .arg("--exclusive")
                        .args(&salloc_opts)
                        .args(&original_args)
                        .status();
                    match salloc_result {
                        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
                        Err(e) => {
                            eprintln!(
                                "hpc_launch: --nodes given but failed to spawn salloc ({}), continuing without allocation",
                                e
                            );
                        }
                    }
                }
            }
        }
    } else {
        quote! {
            let _ = &salloc_opts;
            let _ = &original_args;
            if nodes.is_some() && std::env::var("SLURM_JOB_ID").is_err() {
                eprintln!(
                    "hpc_launch: --nodes given but this binary was built without the \"with-salloc\" feature; not requesting a SLURM allocation."
                );
            }
        }
    };

    let binary_update_block = create_binary_update_block();
    quote! {
        let prte_launched = std::env::var(#env_var).is_ok();
        if !prte_launched {
            // Collect command line arguments
            let mut args: Vec<String> = std::env::args().collect();
            // Full original argv, preserved so we can re-invoke this same binary+args
            // under `salloc` if an allocation needs to be requested first.
            let original_args = args.clone();

            // println!("args: {:?}", args);

            // Remove first argument (executable name) and maintain it for later
            let exec = args.remove(0);
            // println!("exec: {}", exec);

            // Prepare arguments for prterun
            let mut prterun_args = Vec::<String>::new();

            let mut time=false;
            let mut output_dir = String::new();
            let mut gdb_mode: Option<String> = None;
            let mut nodes: Option<u32> = None;
            let mut pes: Option<u32> = None;
            let mut pes_per_node: Option<u32> = None;
            let mut salloc_opts: Vec<String> = Vec::new();
            let mut threads_per_pe_flag: Option<u32> = None;
            let mut log_filter: Option<String> = None;

            // Collect any additional arguments after "--" to pass to prterun
            let pos = args.iter().position(|x| x == "--");
            // println!("-- position: {:?}", pos);
            if let Some(pos) = pos {
                let extra_args: Vec<String> = args.split_off(pos).into_iter().skip(1).collect();
                if extra_args.iter().any(|a| a == "--help" || a == "-h") {
                    println!("hpc_launch launch options (pass after `--`):");
                    print!("{}", ::hpc_launch::generic_help_text());
                    println!("  --help, -h             print this message and exit");
                    println!("Any other flags are passed through verbatim to the underlying launcher (prterun/srun).");
                    std::process::exit(0);
                }
                let mut extra = extra_args.into_iter().peekable();
                while let Some(x) = extra.next() {
                    if x == "--time" {
                        time = true;
                    } else if x == "--output-dir" {
                        if let Some(dir) = extra.next() {
                            output_dir = dir;
                        }
                    } else if x == "--gdb" {
                        if extra.peek().map(|s| s.as_str()) == Some("bt") {
                            extra.next();
                            gdb_mode = Some("bt".to_string());
                        } else {
                            gdb_mode = Some("plain".to_string());
                        }
                    } else if x == "--nodes" {
                        nodes = extra.next().and_then(|n| n.parse().ok());
                    } else if x == "--pes" {
                        pes = extra.next().and_then(|n| n.parse().ok());
                    } else if x == "--pes-per-node" {
                        pes_per_node = extra.next().and_then(|n| n.parse().ok());
                    } else if x == "--threads-per-pe" {
                        threads_per_pe_flag = extra.next().and_then(|n| n.parse().ok());
                    } else if x == "--log" {
                        log_filter = extra.next();
                    } else if x == "--salloc-opts" {
                        // Everything up to the next "--" (or end of args) is passed
                        // through to `salloc` verbatim, e.g.
                        // -- --nodes 2 --salloc-opts --partition foo --time 01:00:00 -- <launcher args>
                        while let Some(y) = extra.next() {
                            if y == "--" {
                                break;
                            }
                            salloc_opts.push(y);
                        }
                    } else {
                        prterun_args.push(x.to_string());
                    }
                }
            }
            let end = args.len();

            if let Some(ref mode) = gdb_mode {
                prterun_args.push("rust-gdb".to_string());
                if mode == "bt" {
                    prterun_args.push("--ex".to_string());
                    prterun_args.push("run".to_string());
                    prterun_args.push("--ex".to_string());
                    prterun_args.push("thread apply all bt full".to_string());
                    prterun_args.push("--ex".to_string());
                    prterun_args.push("quit".to_string());
                }
                prterun_args.push("--args".to_string());
            }

            // Resolve --nodes/--pes/--pes-per-node, falling back to env vars, then
            // deriving whichever of the three wasn't given from the other two.
            let nodes = nodes.or_else(|| std::env::var("LAMELLAR_NODES").ok().and_then(|s| s.parse().ok()));
            let pes = pes.or_else(|| std::env::var("LAMELLAR_PES").ok().and_then(|s| s.parse().ok()));
            let pes_per_node = pes_per_node.or_else(|| std::env::var("LAMELLAR_PES_PER_NODE").ok().and_then(|s| s.parse().ok()));

            let (nodes, pes, pes_per_node): (Option<u32>, Option<u32>, Option<u32>) = match (nodes, pes, pes_per_node) {
                (Some(n), Some(p), Some(ppn)) if n > 0 && ppn > 0 => {
                    if p != n * ppn {
                        eprintln!(
                            "hpc_launch: inconsistent --nodes {} / --pes {} / --pes-per-node {} (expected pes == nodes * pes_per_node)",
                            n, p, ppn
                        );
                        std::process::exit(1);
                    }
                    (Some(n), Some(p), Some(ppn))
                }
                (None, Some(p), Some(ppn)) if ppn > 0 => (Some((p + ppn - 1) / ppn), Some(p), Some(ppn)),
                (Some(n), None, Some(ppn)) if n > 0 => (Some(n), Some(n * ppn), Some(ppn)),
                (Some(n), Some(p), None) if n > 0 => (Some(n), Some(p), Some((p + n - 1) / n)),
                other => other,
            };

            #salloc_block

            // Best-effort NUMA domain count on the current host, used to decide
            // whether to bind PEs by NUMA domain instead of plain node. Absence of
            // a usable hwloc topology just disables NUMA-aware binding.
            let numa_domains: Option<u32> = ::hpc_launch::numa_domain_count();

            // Best-effort physical package (socket) count. PRRTE's binding
            // restriction ("bound to CPUs in more than one package") is keyed on
            // package boundaries, not NUMA domain boundaries -- a package can
            // contain several NUMA domains, so numa_domains must not be used as a
            // stand-in for this check.
            let packages: Option<u32> = ::hpc_launch::package_count();

            println!("hpc_launch: resolved launch parameters: nodes={:?}, pes={:?}, pes_per_node={:?}, threads_per_pe={:?}, numa_domains={:?}, packages={:?}",
                nodes, pes, pes_per_node, threads_per_pe_flag, numa_domains, packages
            );

            // If neither --pes nor --pes-per-node was given, pick a sensible default:
            // one PE per NUMA domain (multiplied by node count if --nodes was given).
            let (pes, pes_per_node) = if pes.is_none() && pes_per_node.is_none() {
                ::hpc_launch::resolve_pe_defaults(nodes, numa_domains, false)
            } else {
                (pes, pes_per_node)
            };

            let threads_per_pe: u32 = threads_per_pe_flag
                .or_else(|| std::env::var("LAMELLAR_THREADS").ok().and_then(|s| s.parse().ok()))
                .unwrap_or_else(|| {
                    let cores = std::thread::available_parallelism()
                        .map(|n| n.get() as u32)
                        .unwrap_or(4);
                    (cores / pes_per_node.unwrap_or(1).max(1)).max(1)
                });

            let has_flag = |args: &Vec<String>, needles: &[&str]| {
                args.iter().any(|a| needles.iter().any(|n| a.starts_with(n)))
            };

            #pe_injection_block

            let mut ld_library_path = std::env::var("LD_LIBRARY_PATH").unwrap_or_else(|_| String::new());


            // println!("initial LD_LIBRARY_PATH: {}", ld_library_path);
            #binary_update_block

            // After the prterun arguments, add the executable name (which may have been updated in place)
            prterun_args.push(exec);

            // Add the arguments targeting the application
            prterun_args.extend(args.into_iter());

            // if !ld_library_path.is_empty() {
            //     ld_library_path.push_str(":");
            // }
            // ld_library_path.push_str(&#shared_libs_dir);
            let mut launcher_cmd = std::process::Command::new(#launcher_path);
            if time {
                launcher_cmd.env("LAMELLAR_MAIN_TIME", "1");
            }
            // Ensure the launched job sees the same threads-per-pe value used to
            // size the launcher's own PE=/--cpus-per-task binding above, even when
            // it was auto-computed here rather than given via --threads-per-pe or
            // an inherited LAMELLAR_THREADS.
            launcher_cmd.env("LAMELLAR_THREADS", threads_per_pe.to_string());
            if let Some(ref filter) = log_filter {
                launcher_cmd.env("LAMELLAR_LOG", filter);
            }
            #output_dir_block
            println!("Launching with {:?}: {:?} {:?}", #env_var, #launcher_path, prterun_args.join(" "));
            launcher_cmd
                // .env("LD_LIBRARY_PATH", ld_library_path)
                .args(prterun_args)
                .status()
                .expect("failed to launch process");
            #ret
        }
    }
}

#[proc_macro_attribute]
pub fn main(_args: TokenStream, item: TokenStream) -> TokenStream {
    #[cfg(not(any(feature = "use-prterun", feature = "use-srun")))]
    compile_error!("Either feature \"use-prterun\" or \"use-srun\" must be enabled for hpc_launch proc macro.");
    #[cfg(all(feature = "use-prterun", feature = "use-srun"))]
    compile_error!("Only one of features \"use-prterun\" or \"use-srun\" can be enabled for hpc_launch proc macro.");

    let func = parse_macro_input!(item as ItemFn);
    assert!(func.sig.asyncness.is_none(), "async not supported");
    assert!(func.sig.constness.is_none(), "const not supported");
    assert!(func.sig.unsafety.is_none(), "unsafety not supported");
    assert!(
        func.sig.generics.lt_token.is_none(),
        "generics not supported"
    );
    assert!(func.sig.variadic.is_none(), "variadics not supported");
    assert!(func.sig.inputs.is_empty(), "inputs not supported");

    #[cfg(any(feature = "use-prterun", feature = "use-srun"))]
    let ret_result = match func.sig.output {
        syn::ReturnType::Default => false,
        syn::ReturnType::Type(_rarrow, ref t) => match t.as_ref() {
            syn::Type::Path(ref type_path) => {
                assert!(
                    type_path.path.segments.first().unwrap().ident.to_string() == "Result",
                    "Only Result<(),...> is supported as return type"
                );
                true
            }
            _other => panic!("Only Result<(),..> is supported as return type"),
        },
    };

    #[cfg(any(feature = "use-prterun", feature = "use-srun"))]
    let ret = if ret_result {
        Some(quote! {Ok(())})
    } else {
        None
    };
    let _output = &func.sig.output;
    let ret_type = func.sig.output;
    let _block = &func.block;
    let stmts = &func.block.stmts;
    let timed_body = quote! {
        {
            #(#stmts)*
        }
    };

    #[cfg(not(any(feature = "use-prterun", feature = "use-srun")))]
    let launch_block = quote! {
        if true {
            panic!("No launch method selected");
        }
    };

    #[cfg(feature = "use-prterun")]
    let launch_block = create_launch_block(
        (quote! {"PRTE_LAUNCHED"}, quote! {::hpc_launch::prrte_sys::prterun_path()}),
        ret,
        "prterun",
    );

    #[cfg(feature = "use-srun")]
    let launch_block =
        create_launch_block((quote! {"SLURM_LOCALID"}, quote! {"srun"}), ret, "srun");

    let res = quote! {
        fn main() #ret_type {
            #launch_block
            else {
                ::hpc_launch::init_tracing_from_env();
                let result = (|| { #timed_body })();
                result
            }
            // println!("hpc_launch: process exiting");
        }
    };
    TokenStream::from(res)
}

#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
fn create_launch_test_block(
    launcher_info: (impl ToTokens, impl ToTokens),
    test_attr: Attribute,
    vis: &syn::Visibility,
    sig: &syn::Signature,
    name: &syn::Ident,
) -> impl ToTokens {
    let (env_var, launcher_path) = launcher_info;
    quote! {
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
            // Collect command line arguments
            let mut args: Vec<String> = std::env::args().collect();

            // Remove first argument (executable name) and maintain it for later
            let exec = args.remove(0);

            // Prepare arguments for prterun
            let mut prterun_args = Vec::<String>::new();

            // Collect any additional arguments after "--" to pass to prterun
            let pos = args.iter().position(|x| x == "--");
            if let Some(pos) = pos {
                args.split_off(pos).into_iter().skip(1).for_each(|x| {
                    prterun_args.push(x.to_string());
                });
            }
            let end = args.len();

            // After the prterun arguments, add the executable name
            prterun_args.push(exec);
            prterun_args.push(func_name);

            // Add the arguments targeting the application
            prterun_args.extend(args.into_iter());
            prterun_args.push("--ignored".to_string());
            prterun_args.push("--exact".to_string());
            // prterun_args.iter().for_each(|x| {
            //     println!("prterun arg: {}", x);
            // });
            std::process::Command::new(#launcher_path)
                .args(prterun_args)
                .status()
                .expect("failed to launch process");
        }
        }
    }
}

#[proc_macro_attribute]
pub fn test(_args: TokenStream, item: TokenStream) -> TokenStream {
    #[cfg(not(any(feature = "use-prterun", feature = "use-srun")))]
    compile_error!("Either feature \"use-prterun\" or \"use-srun\" must be enabled for hpc_launch test proc macro.");
    #[cfg(all(feature = "use-prterun", feature = "use-srun"))]
    compile_error!("Only one of features \"use-prterun\" or \"use-srun\" can be enabled for hpc_launch test proc macro.");

    let func = parse_macro_input!(item as ItemFn);
    assert!(func.sig.asyncness.is_none(), "async not supported");
    assert!(func.sig.constness.is_none(), "const not supported");
    assert!(func.sig.unsafety.is_none(), "unsafety not supported");
    assert!(
        func.sig.generics.lt_token.is_none(),
        "generics not supported"
    );
    assert!(func.sig.variadic.is_none(), "variadics not supported");
    assert!(func.sig.inputs.is_empty(), "inputs not supported");

    #[cfg(any(feature = "use-prterun", feature = "use-srun"))]
    let test_attr: Attribute = parse_quote! {
        #[test]
    };

    let name = syn::Ident::new(
        format!("{}_launched", &func.sig.ident.to_string()).as_str(),
        func.sig.ident.span(),
    );
    #[cfg(any(feature = "use-prterun", feature = "use-srun"))]
    let vis = &func.vis;
    #[cfg(any(feature = "use-prterun", feature = "use-srun"))]
    let sig = &func.sig;

    #[cfg(not(any(feature = "use-prterun", feature = "use-srun")))]
    let res = quote! {
        if true {
            panic!("No launch method selected");
        }
    };

    // let name = format!("{}_launched", name.to_string());
    #[cfg(feature = "use-prterun")]
    let res = create_launch_test_block(
        (quote! {"PRTE_LAUNCHED"}, quote! {::hpc_launch::prrte_sys::prterun_path()}),
        test_attr,
        vis,
        sig,
        &name,
    );

    #[cfg(feature = "use-srun")]
    let res = create_launch_test_block(
        (quote! {"SRUN_LAUNCHED"}, quote! {"srun"}),
        test_attr,
        vis,
        sig,
        &name,
    );

    let launched_attrs: Vec<Attribute> = parse_quote! {
        #[test]
        #[ignore]
    };

    let mut sig_launched = func.sig.clone();
    sig_launched.ident = name;
    let mut func_copy = func.clone();
    func_copy.sig = sig_launched;
    func_copy.attrs = launched_attrs;

    let res = quote! {
        #res
        #func_copy
    };
    TokenStream::from(res)
}
