extern crate proc_macro;

mod am_data;

mod parse;
mod replace;

mod array_ops;
mod array_reduce;

mod gen_am;
mod gen_am_group;

mod field_info;

use am_data::derive_am_data;

use proc_macro::TokenStream;
use proc_macro_error::{abort, emit_error, proc_macro_error};
use quote::{quote, quote_spanned, ToTokens};
use syn::parse_macro_input;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
// use syn::Meta;
// use syn::visit_mut::VisitMut;
use syn::parse::{Parse, ParseStream, Result};
use syn::Token;
fn type_name(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Path(syn::TypePath { qself: None, path }) => {
            Some(path.segments.last().unwrap().ident.to_string())
        }
        _ => None,
    }
}

#[allow(dead_code)]
fn get_impl_associated_type(name: String, tys: &Vec<syn::ImplItem>) -> Option<syn::Type> {
    for ty in tys {
        match ty {
            syn::ImplItem::Type(ref item) => {
                if item.ident.to_string() == name {
                    return Some(item.ty.clone());
                }
            }
            _ => (),
        }
    }
    None
}

fn get_return_of_method(name: String, tys: &Vec<syn::ImplItem>) -> Option<syn::Type> {
    for ty in tys {
        match ty {
            syn::ImplItem::Fn(ref item) => {
                if item.sig.asyncness.is_some() {
                    if item.sig.ident.to_string() == name {
                        match item.sig.output.clone() {
                            syn::ReturnType::Default => {
                                return None;
                            }
                            syn::ReturnType::Type(_, item) => {
                                return Some(*item);
                            }
                        }
                    }
                } else {
                    abort!(item.sig.fn_token.span(),"implementing lamellar::am expects the exec function to be async (e.g. 'async fn exec(...)')")
                }
            }
            _ => (),
        }
    }
    None
}

fn get_impl_method(name: String, tys: &Vec<syn::ImplItem>) -> Option<syn::Block> {
    for ty in tys {
        match ty {
            syn::ImplItem::Fn(ref item) => {
                if item.sig.ident.to_string() == name {
                    return Some(item.block.clone());
                }
            }
            _ => (),
        }
    }
    None
}

fn get_expr(stmt: &syn::Stmt) -> Option<syn::Expr> {
    let expr = match stmt {
        syn::Stmt::Expr(expr, semi) => match expr.clone() {
            syn::Expr::Return(expr) => Some(*(expr.expr.unwrap())),
            _ => {
                if semi.is_some() {
                    None
                } else {
                    Some(expr.clone())
                }
            }
        },
        syn::Stmt::Macro(mac) => {
            abort!(mac.span(),"we currently do not support macros in return position, assign macro output to a variable, and return the variable");
        }
        _ => {
            println!("something else!");
            None
        }
    };
    expr
}

#[derive(Clone)]
enum AmType {
    NoReturn,
    ReturnData(syn::Type),
    ReturnAm(syn::Type, proc_macro2::TokenStream),
}

fn get_return_am_return_type(
    args: &Punctuated<syn::Meta, syn::Token![,]>,
) -> Option<(proc_macro2::TokenStream, proc_macro2::TokenStream)> {
    for arg in args.iter() {
        let arg_str = arg.to_token_stream().to_string();
        if arg_str.contains("return_am") {
            let mut the_am = arg_str
                .split("return_am")
                .collect::<Vec<&str>>()
                .last()
                .expect("error in lamellar::am argument")
                .trim_matches(&['=', ' ', '"'][..])
                .to_string();
            let mut return_type = "".to_string();
            if the_am.find("->") != None {
                let temp = the_am.split("->").collect::<Vec<&str>>();
                return_type = temp
                    .last()
                    .expect("error in lamellar::am argument")
                    .trim()
                    .to_string();
                the_am = temp[0].trim_matches(&[' ', '"'][..]).to_string();
            }
            let ret_am_type: syn::Type = syn::parse_str(&the_am).expect("invalid type");
            if return_type.len() > 0 {
                // let ident = syn::Ident::new(&return_type, Span::call_site());
                let ret_type: syn::Type = syn::parse_str(&return_type).expect("invalid type");
                return Some((
                    quote_spanned! {arg.span() => #ret_am_type},
                    quote_spanned! {arg.span() => #ret_type},
                ));
            } else {
                return Some((quote! {#ret_am_type}, quote! {()}));
            }
        }
    }
    None
}

fn check_for_am_group(args: &Punctuated<syn::Meta, syn::Token![,]>) -> bool {
    for arg in args.iter() {
        let t = arg.to_token_stream().to_string();
        if t.contains("AmGroup") {
            if t.contains("(") {
                let attrs = &t[t.find("(").unwrap()
                    ..t.find(")")
                        .expect("missing \")\" in when declaring ArrayOp macro")
                        + 1];
                if attrs.contains("false") {
                    return false;
                }
            }
        }
    }
    true
}

/// # Examples
///
///```
/// use lamellar::active_messaging::prelude::*;
///
/// #[AmData(Debug,Clone)]
/// struct HelloWorld {
///    originial_pe: usize,
/// }
///```
#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmData(args: TokenStream, input: TokenStream) -> TokenStream {
    let args =
        parse_macro_input!(args with Punctuated<syn::Meta, syn::Token![,]>::parse_terminated);
    // println!("here");
    derive_am_data(input, args, quote! {__lamellar}, false, false, false)
}

/// # Examples
///
///```
/// use lamellar::active_messaging::prelude::*;
///
/// #[AmLocalData(Debug,Clone)]
/// struct HelloWorld {
///    originial_pe: Arc<Mutex<usize>>, //lamellar disallows serializing/deserializing Arc and Mutex
/// }
///```
#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmLocalData(args: TokenStream, input: TokenStream) -> TokenStream {
    let args =
        parse_macro_input!(args with Punctuated<syn::Meta, syn::Token![,]>::parse_terminated);
    derive_am_data(input, args, quote! {__lamellar}, true, false, false)
}

#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmGroupData(args: TokenStream, input: TokenStream) -> TokenStream {
    let args =
        parse_macro_input!(args with Punctuated<syn::Meta, syn::Token![,]>::parse_terminated);
    derive_am_data(input, args, quote! {__lamellar}, false, true, false)
}

#[doc(hidden)]
#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmDataRT(args: TokenStream, input: TokenStream) -> TokenStream {
    let args =
        parse_macro_input!(args with Punctuated<syn::Meta, syn::Token![,]>::parse_terminated);
    derive_am_data(input, args, quote! {crate}, false, false, true)
}

#[doc(hidden)]
#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmLocalDataRT(args: TokenStream, input: TokenStream) -> TokenStream {
    let args =
        parse_macro_input!(args with Punctuated<syn::Meta, syn::Token![,]>::parse_terminated);
    derive_am_data(input, args, quote! {crate}, true, false, true)
}

fn parse_am(
    args: TokenStream,
    input: TokenStream,
    local: bool,
    rt: bool,
    _am_group: bool,
) -> TokenStream {
    let args =
        parse_macro_input!(args with Punctuated<syn::Meta, syn::Token![,]>::parse_terminated);

    // let args = args.to_string();
    // if args.len() > 0 {
    //     if !args.starts_with("return_am") {
    //         abort!(args.span(),"#[lamellar::am] only accepts an (optional) argument of the form:
    //         #[lamellar::am(return_am = \"<am to exec upon return>\")]");
    //     }
    // }
    // println!("args: {:?}", args);
    let input: syn::Item = parse_macro_input!(input);

    let lamellar = if rt {
        // quote::format_ident!("crate")
        quote! {crate}
    } else {
        // quote::format_ident!("__lamellar")
        quote! {__lamellar}
    };

    let am_data_header = if rt {
        if !local {
            quote! {#[lamellar_impl::AmDataRT]}
        } else {
            quote! {#[lamellar_impl::AmLocalDataRT]}
        }
    } else {
        if !local {
            quote! {#[#lamellar::AmData]}
        } else {
            quote! {#[#lamellar::AmLocalData]}
        }
    };

    let am_group_data_header = quote! {#[#lamellar::AmGroupData]};
    let create_am_group = check_for_am_group(&args);

    let output = match input.clone() {
        syn::Item::Impl(input) => {
            let output = get_return_of_method("exec".to_string(), &input.items);
            match output {
                Some(output) => {
                    if let Some((return_am, return_output)) = get_return_am_return_type(&args) {
                        if return_am.to_string() != output.to_token_stream().to_string() {
                            emit_error!(
                                return_am.span(),
                                "am specified in attribute {} does not match return type {}",
                                return_am,
                                output.to_token_stream().to_string()
                            );
                            abort!(
                                output.span(),
                                "am specified in attribute {} does not match return type {}",
                                return_am,
                                output.to_token_stream().to_string()
                            );
                        }
                        let mut impls = gen_am::generate_am(
                            &input,
                            local,
                            AmType::ReturnAm(output.clone(), return_output.clone()),
                            &lamellar,
                            &am_data_header,
                        );
                        if !rt && !local && create_am_group {
                            impls.extend(gen_am_group::generate_am_group(
                                &input,
                                local,
                                AmType::ReturnAm(output.clone(), return_output.clone()),
                                &lamellar,
                                &am_group_data_header,
                            ));
                        }
                        impls
                    } else {
                        let mut impls = gen_am::generate_am(
                            &input,
                            local,
                            AmType::ReturnData(output.clone()),
                            &lamellar,
                            &am_data_header,
                        );
                        if !rt && !local && create_am_group {
                            impls.extend(gen_am_group::generate_am_group(
                                &input,
                                local,
                                AmType::ReturnData(output.clone()),
                                &lamellar,
                                &am_group_data_header,
                            ));
                        }
                        impls
                    }
                }

                None => {
                    let mut impls = gen_am::generate_am(
                        &input,
                        local,
                        AmType::NoReturn,
                        &lamellar,
                        &am_data_header,
                    );
                    if !rt && !local && create_am_group {
                        impls.extend(gen_am_group::generate_am_group(
                            &input,
                            local,
                            AmType::NoReturn,
                            &lamellar,
                            &am_group_data_header,
                        ));
                    }
                    impls
                }
            }
        }
        _ => {
            println!("lamellar am attribute only valid for impl blocks");
            let output = quote! { #input };
            output.into()
        }
    };
    output
}

/// # Examples
///
///```
/// use lamellar::active_messaging::prelude::*;
///
/// #[AmData(Debug,Clone)]
/// struct HelloWorld {
///    originial_pe: Arc<Mutex<usize>>,
/// }
///
/// #[lamellar::am]
/// impl LamellarAM for HelloWorld {
///     async fn exec(self) {
///         println!(
///             "Hello World  on PE {:?} of {:?} using thread {:?}, received from PE {:?}",
///             lamellar::current_pe,
///             lamellar::num_pes,
///             std::thread::current().id(),
///             self.originial_pe.lock(),
///         );
///     }
/// }
/// fn main() {
///     let world = lamellar::LamellarWorldBuilder::new().build();
///     let my_pe = world.my_pe();
///     world.barrier();
///
///     //Send a Hello World Active Message to all pes
///     let request = world.exec_am_all(HelloWorld {
///         originial_pe: Arc::new(Mutex::new(my_pe)),
///     });
///
///     //wait for the request to complete
///     world.block_on(request);
/// } //when world drops there is an implicit world.barrier() that occurs
///```
#[proc_macro_error]
#[proc_macro_attribute]
pub fn am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, false, false, true)
}

#[doc(hidden)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn am_group(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, false, false, false)
}

/// # Examples
///
///```
/// use lamellar::active_messaging::prelude::*;
///
/// #[AmData(Debug,Clone)]
/// struct HelloWorld {
///    originial_pe: usize,
/// }
///
/// #[lamellar::am]
/// impl LamellarAM for HelloWorld {
///     async fn exec(self) {
///         println!(
///             "Hello World  on PE {:?} of {:?} using thread {:?}, received from PE {:?}",
///             lamellar::current_pe,
///             lamellar::num_pes,
///             std::thread::current().id(),
///             self.originial_pe,
///         );
///     }
/// }
/// fn main() {
///     let world = lamellar::LamellarWorldBuilder::new().build();
///     let my_pe = world.my_pe();
///     world.barrier();
///
///     //Send a Hello World Active Message to all pes
///     let request = world.exec_am_all(HelloWorld {
///         originial_pe: my_pe,
///     });
///
///     //wait for the request to complete
///     world.block_on(request);
/// } //when world drops there is an implicit world.barrier() that occurs
///```
#[proc_macro_error]
#[proc_macro_attribute]
pub fn local_am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, true, false, false)
}

#[doc(hidden)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, false, true, false)
}

#[doc(hidden)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am_local(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, true, true, false)
}

#[doc(hidden)]
#[proc_macro_error]
#[proc_macro_derive(Dist)]
pub fn derive_dist(input: TokenStream) -> TokenStream {
    let mut output = quote! {};

    let input = parse_macro_input!(input as syn::DeriveInput);
    let name = input.ident;
    output.extend(quote! {
        const _: () = {
            extern crate lamellar as __lamellar;
            impl __lamellar::Dist for #name {}
        };
    });

    // output.extend(create_ops(name.clone(), &write_array_types, false, false));
    TokenStream::from(output)
}

#[proc_macro_error]
#[proc_macro]
pub fn register_reduction(item: TokenStream) -> TokenStream {
    array_reduce::__register_reduction(item)
}

// probalby should turn this into a derive macro
// #[proc_macro_error]
// #[proc_macro]
// pub fn generate_reductions_for_type(item: TokenStream) -> TokenStream {
//     array_reduce::__generate_reductions_for_type(item)
// }

#[doc(hidden)]
#[proc_macro_error]
#[proc_macro]
pub fn generate_reductions_for_type_rt(item: TokenStream) -> TokenStream {
    array_reduce::__generate_reductions_for_type_rt(item)
}

// / This macro automatically implements various LamellarArray "Op" traits for user defined types
// /
// / The following "Op" traits will be implemented:
// / - [AccessOps][lamellar::array::AccessOps]
// / - [ArithmeticOps][lamellar::array::AccessOps]
// / - [BitWiseOps][lamellar::array::AccessOps]
// / - [CompareExchangeEpsilonOps][lamellar::array::AccessOps]
// / - [CompareExchangeOps][lamellar::array::AccessOps]
// /
// / The required trait bounds can be found by viewing each "Op" traits documentation.
// / Generally though the type must be able to be used in an active message,
// / # Examples
// /
// /```
// / use lamellar::array::prelude::*;
// #[proc_macro_error]
// #[proc_macro]
// pub fn generate_ops_for_type(item: TokenStream) -> TokenStream {
//     array_ops::__generate_ops_for_type(item)
// }

#[doc(hidden)]
#[proc_macro_error]
#[proc_macro]
pub fn generate_ops_for_type_rt(item: TokenStream) -> TokenStream {
    array_ops::__generate_ops_for_type_rt(item)
}

#[doc(hidden)]
#[proc_macro_error]
#[proc_macro]
pub fn generate_ops_for_bool_rt(_item: TokenStream) -> TokenStream {
    array_ops::__generate_ops_for_bool_rt()
}

///
/// This derive macro is intended to be used with the [macro@AmData] attribute macro to enable a user defined type to be used in ActiveMessages.
///
/// # Examples
///
/// ```
/// // this import includes everything we need
/// use lamellar::array::prelude::*;
///
///
/// #[lamellar::AmData(
///     // Lamellar traits
///     ArrayOps(Arithmetic,CompExEps,Shift), // needed to derive various LamellarArray Op traits (provided as a list)
///     Default,       // needed to be able to initialize a LamellarArray
///     //  Notice we use `lamellar::AmData` instead of `derive`
///     //  for common traits, e.g. Debug, Clone.    
///     PartialEq,     // needed for CompareExchangeEpsilonOps
///     PartialOrd,    // needed for CompareExchangeEpsilonOps
///     Debug,         // any addition traits you want derived
///     Clone,
/// )]
/// struct Custom {
///     int: usize,
///     float: f32,
/// }
///
/// // We need to impl various arithmetic ops if we want to be able to
/// // perform remote arithmetic operations with this type
/// impl std::ops::AddAssign for Custom {
///     fn add_assign(&mut self, other: Self) {
///         *self = Self {
///             int: self.int + other.int,
///             float: self.float + other.float,
///         }
///     }
/// }
///
/// impl std::ops::SubAssign for Custom {
///     fn sub_assign(&mut self, other: Self) {
///         *self = Self {
///             int: self.int - other.int,
///             float: self.float - other.float,
///         }
///     }
/// }
///
/// impl std::ops::Sub for Custom {
///     type Output = Self;
///     fn sub(self, other: Self) -> Self {
///         Self {
///             int: self.int - other.int,
///             float: self.float - other.float,
///         }
///     }
/// }
///
/// impl std::ops::MulAssign for Custom {
///     fn mul_assign(&mut self, other: Self) {
///         *self = Self {
///             int: self.int * other.int,
///             float: self.float * other.float,
///         }
///     }
/// }
///
/// impl std::ops::DivAssign for Custom {
///     fn div_assign(&mut self, other: Self) {
///         *self = Self {
///             int: self.int / other.int,
///             float: self.float / other.float,
///         }
///     }
/// }
/// impl std::ops::ShlAssign for Custom {
///     fn shl_assign(&mut self,other: Custom){
///         self.int <<= other.int;
///     }
/// }
///
/// impl std::ops::ShrAssign for Custom {
///     fn shr_assign(&mut self,other: Custom){
///         self.int >>= other.int;
///     }
/// }
///
/// fn main(){
///
///     // initialize
///     // -----------
///     
///     let world = LamellarWorldBuilder::new().build(); // the world
///     
///     let array =  // the atomic distributed array
///         AtomicArray::<Custom>::new(&world,3,Distribution::Block);
///
///     println!();
///     println!("initialize a length-3 array:\n");  // print the entries
///     array.dist_iter()
///         .enumerate()
///         .for_each(|(i,entry)| println!("entry {:?}: {:?}", i, entry ) );
///     array.wait_all();
///     
///     // call various operations on the array!
///     // -------------------------------------
///
///     world.block_on( async move {  // we will just use the world as our future driver so we dont have to deal with cloneing array
///
///         println!();
///         println!("add (1, 0.01) to the first entry:\n");
///         let val = Custom{int: 1, float: 0.01};
///         array.add(0, val ).await;
///         array.dist_iter().enumerate().for_each(|(i,entry)| println!("entry {:?}: {:?}", i, entry ) );
///         array.wait_all();
///
///         println!();
///         println!("batch compare/exchange:");    
///         let indices = vec![0,1,2,];
///         let current = val;
///         let new = Custom{int: 1, float: 0.0};
///         let epsilon = Custom{int: 0, float: 0.01};
///         let _results = array.batch_compare_exchange_epsilon(indices,current,new,epsilon).await;
///         println!();
///         println!("(1) the updatd array");
///         array.dist_iter().enumerate().for_each(|(i,entry)| println!("entry {:?}: {:?}", i, entry ) );
///         array.wait_all();
///         println!();
///         println!("(2) the return values");        
///         for (i, entry) in _results.iter().enumerate() { println!("entry {:?}: {:?}", i, entry ) }
///     });
///
///     // inspect the results
///     // -------------------------------------    
///     // NB:  because thewe're working with multithreaded async
///     //      environments, entries may be printed out of order
///     //
///     // initialize a length-3 array:
///     //
///     // entry 1: Custom { int: 0, float: 0.0 }
///     // entry 0: Custom { int: 0, float: 0.0 }
///     // entry 2: Custom { int: 0, float: 0.0 }
///     //
///     // add (1, 0.01) to the first entry:
///     //
///     // entry 0: Custom { int: 1, float: 0.01 }
///     // entry 2: Custom { int: 0, float: 0.0 }
///     // entry 1: Custom { int: 0, float: 0.0 }
///     //
///     // batch compare/exchange:
///     //
///     // (1) the updatd array
///     // entry 0: Custom { int: 1, float: 0.0 }
///     // entry 1: Custom { int: 0, float: 0.0 }
///     // entry 2: Custom { int: 0, float: 0.0 }
///     //
///     // (2) the return values
///     // entry 0: Ok(Custom { int: 1, float: 0.01 })
///     // entry 1: Err(Custom { int: 0, float: 0.0 })
///     // entry 2: Err(Custom { int: 0, float: 0.0 })   
/// }
/// ```
#[proc_macro_error]
#[proc_macro_derive(ArrayOps, attributes(array_ops))]
pub fn derive_arrayops(input: TokenStream) -> TokenStream {
    array_ops::__derive_arrayops(input)
}

struct AmGroups {
    am: syn::TypePath,
    team: syn::Expr,
}

impl Parse for AmGroups {
    fn parse(input: ParseStream) -> Result<Self> {
        // println!("in am groups parse");
        let am = if let Ok(syn::Type::Path(ty)) = input.parse() {
            ty.clone()
        } else {
            abort!(input.span(),"typed_am_group expects the first argument to be Struct name if your active message e.g. 
            #[AmData]
            Struct MyAmStruct {}
            ...
            typed_am_group!(MyAmStruct,...)");
        };
        // println!("am: {:?}",am);
        input.parse::<Token![,]>()?;
        let team_error_msg = "typed_am_group expects a LamellarWorld or LamellarTeam instance as it's only argument e.g. 
        'typed_am_group!(...,&world)', 
        'typed_am_group!(...,world.clone())'
        'typed_am_group!(...,&team)', 
        'typed_am_group!(...,team.clone())'";
        let team = if let Ok(expr) = input.parse::<syn::Expr>() {
            match expr {
                syn::Expr::Path(_) => expr.clone(),
                syn::Expr::Reference(_) => expr.clone(),
                syn::Expr::MethodCall(_) => expr.clone(),
                _ => abort!(input.span(), team_error_msg),
            }
        } else {
            abort!(input.span(), team_error_msg);
        };
        Ok(AmGroups { am, team })
    }
}

#[proc_macro_error]
#[proc_macro]
pub fn typed_am_group(input: TokenStream) -> TokenStream {
    // println!("typed_am_group {:?}",input);
    let am_group: AmGroups = syn::parse(input).unwrap();
    let am_type = am_group.am;
    let team = am_group.team;
    quote! {
        #am_type::create_am_group(#team)
    }
    .into()
}
