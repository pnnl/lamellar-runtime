
use proc_macro::TokenStream;
// use proc_macro2::TokenStream;
use syn::{parse_macro_input, parse_quote, Attribute, ItemFn};
use quote::quote;
#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
use quote::ToTokens;



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
                    .expect("lamellar_main: failed to create output directory");
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
                    .expect("lamellar_main: failed to create output directory");
                // prterun: use a conservative directive set for compatibility
                // across PRTE/OpenMPI versions.
                
                prterun_args.insert(
                    0,
                    format!("--merge-stderr-to-stdout"),
                );
                prterun_args.insert(
                    0,
                    format!("--output=directory={}", __dir),
                );
            }
        }
    };

    quote! {
        let prte_launched = std::env::var(#env_var).is_ok();
        if !prte_launched {
            // Collect command line arguments
            let mut args: Vec<String> = std::env::args().collect();

            // Remove first argument (executable name) and maintain it for later
            let exec = args.remove(0);

            // Prepare arguments for prterun
            let mut prterun_args = Vec::<String>::new();

            let mut time=false;
            let mut output_dir = String::new();
            let mut gdb_mode: Option<String> = None;

            // Collect any additional arguments after "--" to pass to prterun
            let pos = args.iter().position(|x| x == "--");
            if let Some(pos) = pos {
                let mut extra = args.split_off(pos).into_iter().skip(1).peekable();
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

            // After the prterun arguments, add the executable name
            prterun_args.push(exec);

            // Add the arguments targeting the application
            prterun_args.extend(args.into_iter());

            let mut ld_library_path = std::env::var("LD_LIBRARY_PATH").unwrap_or_else(|_| String::new());
            if let Ok(origin) = std::env::var("ORIGIN"){
                if !ld_library_path.is_empty() {
                    ld_library_path.push_str(":");
                }
                ld_library_path.push_str(&origin);
            }
            let mut launcher_cmd = std::process::Command::new(#launcher_path);
            if time {
                launcher_cmd.env("LAMELLAR_MAIN_TIME", "1");
            }
            #output_dir_block
            println!("Launching with {:?}: {:?} {:?}", #env_var, #launcher_path, prterun_args.join(" "));
            launcher_cmd
                .env("LD_LIBRARY_PATH", ld_library_path)
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
    assert!(func.sig.generics.lt_token.is_none(), "generics not supported");
    assert!(func.sig.variadic.is_none(), "variadics not supported");
    assert!(func.sig.inputs.is_empty(), "inputs not supported");
    
    #[cfg(any(feature = "use-prterun", feature = "use-srun"))]
    let ret_result = match func.sig.output {
        syn::ReturnType::Default => false,
        syn::ReturnType::Type(_rarrow, ref t) => match t.as_ref() {
            syn::Type::Path(ref type_path) => {assert!(type_path.path.segments.first().unwrap().ident.to_string() == "Result", "Only Result<(),...> is supported as return type"); true},
            _other => panic!("Only Result<(),..> is supported as return type"),
        },
    };

    #[cfg(any(feature = "use-prterun", feature = "use-srun"))]
    let ret = if ret_result {Some(quote! {Ok(())})} else {None};
    let _output = &func.sig.output;
    let ret_type = func.sig.output;
    let block = &func.block;

    #[cfg(not(any(feature = "use-prterun", feature = "use-srun")))]
    let launch_block = quote! {
        if true {
            panic!("No launch method selected");
        }
    };
    
    #[cfg(feature = "use-prterun")]
    let launch_block = create_launch_block((quote! {"PRTE_LAUNCHED"}, quote! {prterun_path()}), ret, "prterun");

    #[cfg(feature = "use-srun")]
    let launch_block = create_launch_block((quote! {"SLURM_LOCALID"}, quote! {"srun"}), ret, "srun");
    
    let import = if cfg!(feature = "use-prterun") {
        quote! {
            use prrte_sys::prterun_path;
        }
    } else {
        quote! {}
    };

    // Output redirection is handled at the launcher level so capture starts before
    // user code executes (including C library output during initialization).
    // srun uses --output/--error, while prterun uses --output directives.
    #[cfg(any(feature = "use-srun", feature = "use-prterun"))]
    let pe_output_dir_block = quote! {};

    let res = quote! {
        #import

        fn main() #ret_type {
            #launch_block
            else {
                #pe_output_dir_block
                if let Ok(__lamellar_log) = std::env::var("LAMELLAR_LOG") {
                    if !__lamellar_log.trim().is_empty() {
                        use ::lamellar::tracing_subscriber::prelude::*;

                        std::env::set_var("RUST_LOG", &__lamellar_log);
                        let _ = ::lamellar::tracing_subscriber::registry()
                            .with(::lamellar::tracing_subscriber::EnvFilter::from_default_env())
                            .with(
                                ::lamellar::tracing_subscriber::fmt::layer()
                                    .with_thread_ids(true)
                                    .with_file(true)
                                    .with_line_number(true)
                                    .with_level(true),
                            )
                            .try_init();
                    }
                }
                let mut __lamellar_main_timer = std::time::Instant::now();
                let result = (|| #block)();
                if std::env::var("LAMELLAR_MAIN_TIME").is_ok() {
                    let __lamellar_main_duration = __lamellar_main_timer.elapsed();
                    println!("[LAMELLAR_MAIN] execution time: {:?}", __lamellar_main_duration);
                }
                result
            }
        } 
    };
    TokenStream::from(res)
}

#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
fn create_launch_test_block(launcher_info: (impl ToTokens, impl ToTokens), test_attr: Attribute, vis: &syn::Visibility, sig: &syn::Signature, name: &syn::Ident, imports: impl ToTokens) -> impl ToTokens {
    let (env_var, launcher_path) = launcher_info;
    quote! {
        #test_attr #vis #sig {
        fn type_name_of_val<T: ?Sized>(_val: &T) -> &'static str {
            std::any::type_name::<T>()
        }
        #imports

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
    compile_error!("Either feature \"use-prterun\" or \"use-srun\" must be enabled for lamellar_test proc macro.");
    #[cfg(all(feature = "use-prterun", feature = "use-srun"))]
    compile_error!("Only one of features \"use-prterun\" or \"use-srun\" can be enabled for lamellar_test proc macro.");

    let func = parse_macro_input!(item as ItemFn);
    assert!(func.sig.asyncness.is_none(), "async not supported");
    assert!(func.sig.constness.is_none(), "const not supported");
    assert!(func.sig.unsafety.is_none(), "unsafety not supported");
    assert!(func.sig.generics.lt_token.is_none(), "generics not supported");
    assert!(func.sig.variadic.is_none(), "variadics not supported");
    assert!(func.sig.inputs.is_empty(), "inputs not supported");

    #[cfg(any(feature = "use-prterun", feature = "use-srun"))]
    let test_attr: Attribute = parse_quote!{
        #[test]
    };

    let name = syn::Ident::new(format!("{}_launched", &func.sig.ident.to_string()).as_str(), func.sig.ident.span());
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
    let res = create_launch_test_block((quote! {"PRTE_LAUNCHED"}, quote! {prterun_path()}), test_attr, vis, sig, &name, quote! {use prrte_sys::prterun_path;});

    #[cfg(feature = "use-srun")]
    let res = create_launch_test_block((quote! {"SRUN_LAUNCHED"}, quote! {"srun"}), test_attr, vis, sig, &name, quote! {});

        
    let launched_attrs: Vec<Attribute> = parse_quote!{
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

