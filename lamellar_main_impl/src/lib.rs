use proc_macro::TokenStream;
use quote::quote;
#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
use quote::ToTokens;
use syn::{parse_macro_input, ItemFn};

#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
fn create_launch_block(
    launcher_info: (impl ToTokens, impl ToTokens),
    ret: Option<impl ToTokens>,
    launcher_type: &str,
) -> impl ToTokens {
    let (env_var, launcher_path) = launcher_info;

    let output_dir_block = if launcher_type == "srun" {
        quote! {
            if !output_dir.is_empty() {
                let __dir = output_dir.trim_end_matches('/');
                std::fs::create_dir_all(__dir)
                    .expect("lamellar_main: failed to create output directory");
                prterun_args.insert(0, format!("--error={}/pe_%t.err", __dir));
                prterun_args.insert(0, format!("--output={}/pe_%t.out", __dir));
            }
        }
    } else {
        quote! {
            if !output_dir.is_empty() {
                let __dir = output_dir.trim_end_matches('/');
                std::fs::create_dir_all(__dir)
                    .expect("lamellar_main: failed to create output directory");
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
        quote! {
            if pes.is_some() || pes_per_node.is_some() {
                if let Some(p) = pes {
                    if !has_flag(&prterun_args, &["--np", "-np"]) {
                        prterun_args.push("--np".to_string());
                        prterun_args.push(p.to_string());
                    }
                }
                if pes_per_node.is_some() && !has_flag(&prterun_args, &["--map-by"]) {
                    // pes_per_node < packages alone doesn't mean a PE must span
                    // packages -- it only might, if threads_per_pe needs more cores
                    // than one package has. Prefer PE=<threads_per_pe> binding
                    // whenever it still fits in a single package; only give up
                    // binding entirely when it can't.
                    let spans_multiple_domains = match packages {
                        Some(p) if p > 1 && pes_per_node.unwrap_or(1) < p => {
                            match ::lamellar::cores_per_package() {
                                Some(cores_per_pkg) => threads_per_pe > cores_per_pkg,
                                None => true,
                            }
                        }
                        _ => false,
                    };
                    prterun_args.push("--map-by".to_string());
                    if spans_multiple_domains {
                        prterun_args.push("node".to_string());
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
                                "lamellar_main: --nodes given but failed to spawn salloc ({}), continuing without allocation",
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
                    "lamellar_main: --nodes given but this binary was built without the \"with-salloc\" feature; not requesting a SLURM allocation."
                );
            }
        }
    };

    let binary_update_block = ::hpc_launch::binary_update_block_tokens();
    quote! {
        let prte_launched = std::env::var(#env_var).is_ok();
        if !prte_launched {
            let mut args: Vec<String> = std::env::args().collect();
            let original_args = args.clone();

            let exec = args.remove(0);

            let mut prterun_args = Vec::<String>::new();

            let mut time=false;
            let mut output_dir = String::new();
            let mut gdb_mode: Option<String> = None;
            let mut nodes: Option<u32> = None;
            let mut pes: Option<u32> = None;
            let mut pes_per_node: Option<u32> = None;
            let mut salloc_opts: Vec<String> = Vec::new();
            let mut threads_per_pe_flag: Option<u32> = None;
            let mut lamellae: Option<String> = None;
            let mut cmd_queue: Option<String> = None;
            let mut batcher: Option<String> = None;
            let mut executor: Option<String> = None;
            let mut log_filter: Option<String> = None;
            let mut heap_size: Option<String> = None;

            // `cargo run --example X -- <args>` only lets one literal "--" through
            // to the binary (cargo consumes its own args/binary-args separator), so
            // requiring users to type a second "--" to mark the launch-options
            // section is easy to forget. Fall back to splitting at the first
            // recognized launch flag when no literal "--" is present at all, so
            // `cargo run --example X -- --pes 2 --lamellae shmem` still works.
            const RESERVED_LAUNCH_FLAGS: &[&str] = &[
                "--time", "--output-dir", "--gdb", "--nodes", "--pes", "--pes-per-node",
                "--threads-per-pe", "--lamellae", "--cmd-queue", "--batcher", "--executor",
                "--log", "--heap-size", "--salloc-opts", "--help", "-h",
            ];
            let pos = args
                .iter()
                .position(|x| x == "--")
                .or_else(|| args.iter().position(|x| RESERVED_LAUNCH_FLAGS.contains(&x.as_str())));
            if let Some(pos) = pos {
                let has_explicit_sep = args.get(pos).map(|x| x == "--").unwrap_or(false);
                let extra_args: Vec<String> = if has_explicit_sep {
                    args.split_off(pos).into_iter().skip(1).collect()
                } else {
                    args.split_off(pos)
                };
                if extra_args.iter().any(|a| a == "--help" || a == "-h") {
                    println!("lamellar_main launch options (pass after `--`):");
                    print!("{}", ::lamellar::generic_help_text());
                    println!("  --lamellae <name>      lamellae backend for the launched job [env: LAMELLAR_BACKEND]");
                    println!("      available: {} (default: {})", ::lamellar::available_backends().join(", "), ::lamellar::compiled_default_backend());
                    println!("  --cmd-queue <variant>  command queue protocol for the launched job [env: LAMELLAR_CMD_QUEUE]");
                    println!("      available: batched, get (default), geteager, getslots, put, putslots, puteager");
                    println!("  --batcher <variant>    active-message batcher for the launched job [env: LAMELLAR_BATCHER]");
                    println!("      available: simple (default), direct, team_am, vec_simple (experimental), vec_team_am (experimental)");
                    println!("  --executor <variant>   executor used during execution for the launched job [env: LAMELLAR_EXECUTOR]");
                    println!("      available: lamellar (default), single_thread, async_std, tokio (if tokio-executor feature enabled)");
                    println!("  --heap-size <bytes>    initial size of the RDMAable runtime memory pool [env: LAMELLAR_HEAP_SIZE]");
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
                    } else if x == "--lamellae" {
                        lamellae = extra.next();
                    } else if x == "--cmd-queue" {
                        cmd_queue = extra.next();
                    } else if x == "--batcher" {
                        batcher = extra.next();
                    } else if x == "--executor" {
                        executor = extra.next();
                    } else if x == "--log" {
                        log_filter = extra.next();
                    } else if x == "--heap-size" {
                        heap_size = extra.next();
                    } else if x == "--salloc-opts" {
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

            let nodes = nodes.or_else(|| std::env::var("LAMELLAR_NODES").ok().and_then(|s| s.parse().ok()));
            let pes = pes.or_else(|| std::env::var("LAMELLAR_PES").ok().and_then(|s| s.parse().ok()));
            let pes_per_node = pes_per_node.or_else(|| std::env::var("LAMELLAR_PES_PER_NODE").ok().and_then(|s| s.parse().ok()));

            let (nodes, pes, pes_per_node): (Option<u32>, Option<u32>, Option<u32>) = match (nodes, pes, pes_per_node) {
                (Some(n), Some(p), Some(ppn)) if n > 0 && ppn > 0 => {
                    if p != n * ppn {
                        eprintln!(
                            "lamellar_main: inconsistent --nodes {} / --pes {} / --pes-per-node {} (expected pes == nodes * pes_per_node)",
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

            let numa_domains: Option<u32> = ::lamellar::numa_domain_count();
            let packages: Option<u32> = ::lamellar::package_count();

            println!("lamellar_main: resolved launch parameters: nodes={:?}, pes={:?}, pes_per_node={:?}, threads_per_pe={:?}, numa_domains={:?}, packages={:?}",
                nodes, pes, pes_per_node, threads_per_pe_flag, numa_domains, packages
            );

            let (pes, pes_per_node) = if pes.is_none() && pes_per_node.is_none() {
                ::lamellar::resolve_pe_defaults(nodes, numa_domains, ::lamellar::config().backend == "local")
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

            #binary_update_block

            prterun_args.push(exec);

            prterun_args.extend(args.into_iter());

            let mut launcher_cmd = std::process::Command::new(#launcher_path);
            if time {
                launcher_cmd.env("LAMELLAR_MAIN_TIME", "1");
            }
            if let Some(ref backend) = lamellae {
                launcher_cmd.env("LAMELLAR_BACKEND", backend);
            }
            if let Some(ref cq) = cmd_queue {
                launcher_cmd.env("LAMELLAR_CMD_QUEUE", cq);
            }
            if let Some(ref b) = batcher {
                launcher_cmd.env("LAMELLAR_BATCHER", b);
            }
            if let Some(ref e) = executor {
                launcher_cmd.env("LAMELLAR_EXECUTOR", e);
            }
            // Ensure the launched job sees the same threads-per-pe value used to
            // size the launcher's own PE=/--cpus-per-task binding above, even when
            // it was auto-computed here rather than given via --threads-per-pe or
            // an inherited LAMELLAR_THREADS.
            launcher_cmd.env("LAMELLAR_THREADS", threads_per_pe.to_string());
            if let Some(ref filter) = log_filter {
                launcher_cmd.env("LAMELLAR_LOG", filter);
            }
            if let Some(ref hs) = heap_size {
                launcher_cmd.env("LAMELLAR_HEAP_SIZE", hs);
            }
            #output_dir_block
            println!("Launching with {:?}: {:?} {:?}", #env_var, #launcher_path, prterun_args.join(" "));
            launcher_cmd
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
    compile_error!("Either feature \"use-prterun\" or \"use-srun\" must be enabled for lamellar_main proc macro.");
    #[cfg(all(feature = "use-prterun", feature = "use-srun"))]
    compile_error!("Only one of features \"use-prterun\" or \"use-srun\" can be enabled for lamellar_main proc macro.");

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
    let ret_type = func.sig.output;

    // Find the LamellarWorldBuilder::new()...build() statement and split the
    // block around it so we can emit a separate timer for world construction
    // and one for the application code that follows.
    let world_build_idx = func
        .block
        .stmts
        .iter()
        .position(|stmt| quote!(#stmt).to_string().contains("LamellarWorldBuilder"));

    let timed_body = if let Some(idx) = world_build_idx {
        let pre = &func.block.stmts[..idx];
        let world_stmt = &func.block.stmts[idx];
        let post = &func.block.stmts[idx + 1..];
        quote! {
            #(#pre)*
            let __lamellar_world_build_start = std::time::Instant::now();
            let mut __lamellar_app_start;
            {
                #world_stmt
                if std::env::var("LAMELLAR_MAIN_TIME").is_ok() {
                    println!("[LAMELLAR_MAIN] world build time: {:?}", __lamellar_world_build_start.elapsed());
                }
                __lamellar_app_start = std::time::Instant::now();
                #(#post)*

            } // this should enforce that world is dropped across all PEs before we print the application time, which is important for accurate timing of the application code.
            if std::env::var("LAMELLAR_MAIN_TIME").is_ok() {
                println!("[LAMELLAR_MAIN] application time: {:?}", __lamellar_app_start.elapsed());
            }
        }
    } else {
        let stmts = &func.block.stmts;
        quote! {
            {
                #(#stmts)*
            }
        }
    };

    let (init_prof, fini_prof) = if cfg!(feature = "enable-prof") {
        (
            quote! {
                ::lamellar::init_prof_bt!();
            },
            quote! {
                ::lamellar::fini_prof!();
            },
        )
    } else {
        (quote! {}, quote! {})
    };

    #[cfg(not(any(feature = "use-prterun", feature = "use-srun")))]
    let launch_block = quote! {
        if true {
            panic!("No launch method selected");
        }
    };

    #[cfg(feature = "use-prterun")]
    let launch_block = create_launch_block(
        (quote! {"PRTE_LAUNCHED"}, quote! {::lamellar::prrte_sys::prterun_path()}),
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
                ::lamellar::init_tracing_from_env();
                #init_prof
                let result = (|| { #timed_body })();
                #fini_prof
                result
            }
        }
    };
    TokenStream::from(res)
}
