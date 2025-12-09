
use proc_macro::TokenStream;
// use proc_macro2::TokenStream;
use syn::{parse_macro_input, parse_quote, Attribute, ItemFn};
use quote::{quote, ToTokens};

#[cfg(feature = "use-prterun")]
fn launch_prterun(ret: Option<impl ToTokens>) -> impl ToTokens {
    quote! {
        let prte_launched = std::env::var("PRTE_LAUNCHED").is_ok();
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

            std::process::Command::new(prterun_path())
                .args(prterun_args)
                .status()
                .expect("failed to launch process");
            #ret
        }
    }
}


#[cfg(feature = "use-srun")]
fn launch_srun(ret: Option<impl ToTokens>) -> impl ToTokens {
    quote! {
        let slurm_launched = std::env::var("SLURM_LOCALID").is_ok();
        if !slurm_launched {
            // Collect command line arguments
            let mut args: Vec<String> = std::env::args().collect();

            // Remove first argument (executable name) and maintain it for later
            let exec = args.remove(0);

            // Prepare arguments for prterun
            let mut slurmrun_args = Vec::<String>::new();

            // Collect any additional arguments after "--" to pass to prterun
            let pos = args.iter().position(|x| x == "--");
            if let Some(pos) = pos {
                args.split_off(pos).into_iter().skip(1).for_each(|x| {
                    slurmrun_args.push(x.to_string());
                });
            }
            let end = args.len();

            // After the prterun arguments, add the executable name
            slurmrun_args.push(exec);
            
            // Add the arguments targeting the application
            slurmrun_args.extend(args.into_iter());

            std::process::Command::new("srun")
                .args(slurmrun_args)
                .status()
                .expect("failed to launch process");
            #ret
        }
    }
}

#[proc_macro_attribute]
pub fn lamellar_main(_args: TokenStream, item: TokenStream) -> TokenStream {

    // let mut source = item.clone().into_iter().peekable();
    let func = parse_macro_input!(item as ItemFn);
    assert!(func.sig.asyncness.is_none(), "async not supported");
    assert!(func.sig.constness.is_none(), "const not supported");
    assert!(func.sig.unsafety.is_none(), "unsafety not supported");
    assert!(func.sig.generics.lt_token.is_none(), "generics not supported");
    assert!(func.sig.variadic.is_none(), "variadics not supported");
    assert!(func.sig.inputs.is_empty(), "inputs not supported");
    let ret_result = match func.sig.output {
        syn::ReturnType::Default => false,
        syn::ReturnType::Type(_rarrow, ref t) => match t.as_ref() {
            syn::Type::Path(ref type_path) => {assert!(type_path.path.segments.first().unwrap().ident.to_string() == "Result", "Only Result<(),...> is supported as return type"); true},
            _other => panic!("Only Result<(),..> is supported as return type"),
        },
    };
    let ret = if ret_result {Some(quote! {Ok(())})} else {None};
    let _output = &func.sig.output;
    let ret_type = func.sig.output;
    let block = &func.block;

    let launch_block = quote! {
        if true {
            panic!("No launch method selected");
        }
    };

    #[cfg(feature = "use-prterun")]
    let launch_block = launch_prterun(ret);
    
    #[cfg(feature = "use-srun")]
    let launch_block = launch_srun(ret);
    
    let import = quote! {};
    #[cfg(feature = "use-prterun")]
    let import = quote! {
        use prrte_sys::prterun_path;
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

#[proc_macro_attribute]
pub fn lamellar_test(_args: TokenStream, item: TokenStream) -> TokenStream {

    // let mut source = item.clone().into_iter().peekable();
    let func = parse_macro_input!(item as ItemFn);
    println!("Function name: {}", func.sig.ident.to_string());
    assert!(func.sig.asyncness.is_none(), "async not supported");
    assert!(func.sig.constness.is_none(), "const not supported");
    assert!(func.sig.unsafety.is_none(), "unsafety not supported");
    assert!(func.sig.generics.lt_token.is_none(), "generics not supported");
    assert!(func.sig.variadic.is_none(), "variadics not supported");
    assert!(func.sig.inputs.is_empty(), "inputs not supported");

    let test_attr: Attribute = parse_quote!{
        #[test]
    };

    let name = syn::Ident::new(format!("{}_launched", &func.sig.ident.to_string()).as_str(), func.sig.ident.span());
    let vis = &func.vis;
    let sig = &func.sig;


    // let name = format!("{}_launched", name.to_string());
    let res = quote! {
        #test_attr #vis #sig {
        fn type_name_of_val<T: ?Sized>(_val: &T) -> &'static str {
            std::any::type_name::<T>()
        }
        use prrte_sys::prterun_path;

        let full_func_name = type_name_of_val(&#name);
        let func_name = full_func_name
            .split("::")
            .into_iter()
            .skip(1)
            .collect::<Vec<&str>>()
            .join("::");

        let prte_launched = std::env::var("PRTE_LAUNCHED").is_ok();
        if !prte_launched {
        //     // Collect command line arguments
            let mut args: Vec<String> = std::env::args().collect();

        //     // Remove first argument (executable name) and maintain it for later
            let exec = args.remove(0);

        //     // Prepare arguments for prterun
            let mut prterun_args = Vec::<String>::new();

        //     // Collect any additional arguments after "--" to pass to prterun
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
            std::process::Command::new(prterun_path())
                .args(prterun_args)
                .status()
                .expect("failed to launch process");
        }
        }
    };

        
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

