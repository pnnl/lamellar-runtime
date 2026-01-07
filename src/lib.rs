
use proc_macro::TokenStream;
// use proc_macro2::TokenStream;
use syn::{parse_macro_input, parse_quote, Attribute, ItemFn};
use quote::quote;
#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
use quote::ToTokens;

#[cfg(any(feature = "use-prterun", feature = "use-srun"))]
fn create_launch_block(launcher_info: (impl ToTokens, impl ToTokens), ret: Option<impl ToTokens>) -> impl ToTokens {

    let (env_var, launcher_path) = launcher_info;
    quote! {
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
            
            // Add the arguments targeting the application
            prterun_args.extend(args.into_iter());

            std::process::Command::new(#launcher_path)
                .args(prterun_args)
                .status()
                .expect("failed to launch process");
            #ret
        }
    }
}

#[proc_macro_attribute]
pub fn lamellar_main(_args: TokenStream, item: TokenStream) -> TokenStream {
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
    let launch_block = create_launch_block((quote! {"PRTE_LAUNCHED"}, quote! {prterun_path()}), ret);
    
    #[cfg(feature = "use-srun")]
    let launch_block = create_launch_block((quote! {"SLURM_LOCALID"}, quote! {"srun"}), ret);
    
    let import = if cfg!(feature = "use-prterun") {
        quote! {
            use prrte_sys::prterun_path;
        }
    } else {
        quote! {}
    };

    let res = quote! {
        #import
        
        fn main() #ret_type {
            #launch_block
            else {
                #block
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
pub fn lamellar_test(_args: TokenStream, item: TokenStream) -> TokenStream {
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

