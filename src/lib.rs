use proc_macro::TokenStream;
use syn::{parse_macro_input, token::Type, ItemConst, ItemFn};
use quote::quote;

#[proc_macro_attribute]
pub fn lamellar_main(args: TokenStream, item: TokenStream) -> TokenStream {

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

    let res = quote! {
        use prrte_sys::prterun_path;
        
        fn main() #ret_type {
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
            else {
                #block
            }
        } 
    };
    TokenStream::from(res)
}