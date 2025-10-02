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
        if let syn::ImplItem::Type(ref item) = ty {
            if item.ident == name {
                return Some(item.ty.clone());
            }
        }
    }
    None
}

fn get_return_of_method(name: String, tys: &Vec<syn::ImplItem>) -> Option<syn::Type> {
    for ty in tys {
        if let syn::ImplItem::Fn(ref item) = ty {
            if item.sig.asyncness.is_some() {
                if item.sig.ident == name {
                    return match item.sig.output.clone() {
                        syn::ReturnType::Default => None,
                        syn::ReturnType::Type(_, item) => Some(*item),
                    };
                }
            } else {
                abort!(item.sig.fn_token.span(),"implementing lamellar::am expects the exec function to be async (e.g. 'async fn exec(...)')")
            }
        }
    }
    None
}

fn get_impl_method(name: String, tys: &Vec<syn::ImplItem>) -> Option<syn::Block> {
    for ty in tys {
        if let syn::ImplItem::Fn(ref item) = ty {
            if item.sig.ident == name {
                return Some(item.block.clone());
            }
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
            if the_am.contains("->") {
                let temp = the_am.split("->").collect::<Vec<&str>>();
                return_type = temp
                    .last()
                    .expect("error in lamellar::am argument")
                    .trim()
                    .to_string();
                the_am = temp[0].trim_matches(&[' ', '"'][..]).to_string();
            }
            let ret_am_type: syn::Type = syn::parse_str(&the_am).expect("invalid type");
            if !return_type.is_empty() {
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

fn check_for_am_group(args: &Punctuated<syn::Meta, Token![,]>) -> bool {
    for arg in args.iter() {
        let t = arg.to_token_stream().to_string();
        if t.contains("AmGroup") && t.contains("(") {
            let attrs = &t[t.find("(").unwrap()
                ..t.find(")")
                    .expect("missing \")\" in when declaring ArrayOp macro")
                    + 1];
            if attrs.contains("false") {
                return false;
            }
        }
    }
    true
}

/// # Examples
///
///```
/// use lamellar::active_messaging::prelude::*;
/// use lamellar::darc::prelude::*;
///
/// #[AmData(Debug,Clone)]
/// struct HelloWorld {
///    original_pe: usize,
///    #[AmGroup(static)]
///    msg: Darc<String>,
/// }
///
/// #[lamellar::am]
/// impl LamellarAM for HelloWorld {
///     async fn exec(self) {
///         println!(
///             "{:?}  on PE {:?} of {:?} using thread {:?}, received from PE {}",
///             self.msg,
///             lamellar::current_pe,
///             lamellar::num_pes,
///             std::thread::current().id(),
///             self.original_pe,
///         );
///     }
/// }
/// fn main() {
///     let world = lamellar::LamellarWorldBuilder::new().build();
///     let my_pe = world.my_pe();
///     world.barrier();
///     let msg = Darc::<String>::new(&world, "Hello World".to_string()).block().unwrap();
///     //Send a Hello World Active Message to all pes
///     let request = world.exec_am_all(HelloWorld {
///         original_pe: my_pe,
///         msg: msg,
///     });
///
///     //wait for the request to complete
///     request.block();
/// } //when world drops there is an implicit world.barrier() that occurs
///```
#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmData(args: TokenStream, input: TokenStream) -> TokenStream {
    let args =
        parse_macro_input!(args with Punctuated<syn::Meta, syn::Token![,]>::parse_terminated);
    derive_am_data(input, args, quote! {__lamellar}, false, false, false)
}

///```
/// use lamellar::active_messaging::prelude::*;
/// use std::sync::{Arc, Mutex};
///
/// #[AmLocalData(Debug,Clone)]
/// struct HelloWorld {
///     original_pe: Arc<Mutex<usize>>, //This would not be allowed in a non-local AM as Arc<Mutex<<>> is not (de)serializable
/// }
///
/// #[lamellar::local_am]
/// impl LamellarAM for HelloWorld {
///     async fn exec(self) {
///         println!(
///             "Hello World  on PE {:?} of {:?} using thread {:?}, received from PE {:?}",
///             lamellar::current_pe,
///             lamellar::num_pes,
///             std::thread::current().id(),
///             self.original_pe.lock(),
///         );
///     }
/// }
///
/// let world = lamellar::LamellarWorldBuilder::new().build();
/// let my_pe = Arc::new(Mutex::new(world.my_pe()));
/// world.barrier();
///
/// let request = world.exec_am_local(HelloWorld {
///     original_pe: my_pe,
/// });
///
/// //wait for the request to complete
/// request.block();
/// //when `world` drops there is an implicit world.barrier() that occurs
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

//#[doc(hidden)]
#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmDataRT(args: TokenStream, input: TokenStream) -> TokenStream {
    let args =
        parse_macro_input!(args with Punctuated<syn::Meta, syn::Token![,]>::parse_terminated);
    derive_am_data(input, args, quote! {crate}, false, false, true)
}

//#[doc(hidden)]
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
    } else if !local {
        quote! {#[#lamellar::AmData]}
    } else {
        quote! {#[#lamellar::AmLocalData]}
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
/// use lamellar::darc::prelude::*;
///
/// #[AmData(Debug,Clone)]
/// struct HelloWorld {
///    originial_pe: usize,
///    #[AmGroup(static)]
///    msg: Darc<String>,
/// }
///
/// #[lamellar::am]
/// impl LamellarAM for HelloWorld {
///     async fn exec(self) {
///         println!(
///             "{:?}  on PE {:?} of {:?} using thread {:?}, received from PE {}",
///             self.msg,
///             lamellar::current_pe,
///             lamellar::num_pes,
///             std::thread::current().id(),
///             self.original_pe,
///         );
///     }
/// }
/// fn main() {
///     let world = lamellar::LamellarWorldBuilder::new().build();
///     let my_pe = world.my_pe();
///     world.barrier();
///     let msg = Darc::<String>::new(&world, "Hello World".to_string()).block().unwrap();
///     //Send a Hello World Active Message to all pes
///     let request = world.exec_am_all(HelloWorld {
///         originial_pe: my_pe,
///         msg: msg.into(),
///     });
///
///     //wait for the request to complete
///     request.block();
/// } //when world drops there is an implicit world.barrier() that occurs
///```
#[proc_macro_error]
#[proc_macro_attribute]
pub fn am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, false, false, true)
}

//#[doc(hidden)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn am_group(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, false, false, false)
}

/// # Examples
///
///```
/// use lamellar::active_messaging::prelude::*;
/// use std::sync::{Arc, Mutex};
///
/// use std::sync::{Arc, Mutex};
///
/// #[AmLocalData(Debug,Clone)]
/// struct HelloWorld {
///     originial_pe: Arc<Mutex<usize>>, //This would not be allowed in a non-local AM as Arc<Mutex<<>> is not (de)serializable
/// }
///
/// #[lamellar::local_am]
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
///     let my_pe = Arc::new(Mutex::new(world.my_pe()));
///     world.barrier();
///
///     let request = world.exec_am_local(HelloWorld {
///         originial_pe: my_pe,
///     });
///
///     //wait for the request to complete
///     request.block();
/// } //when world drops there is an implicit world.barrier() that occurs
///```
#[proc_macro_error]
#[proc_macro_attribute]
pub fn local_am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, true, false, false)
}

//#[doc(hidden)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, false, true, false)
}

//#[doc(hidden)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am_local(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, true, true, false)
}

//#[doc(hidden)]
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

//#[doc(hidden)]
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

//#[doc(hidden)]
#[proc_macro_error]
#[proc_macro]
pub fn generate_ops_for_type_rt(item: TokenStream) -> TokenStream {
    array_ops::__generate_ops_for_type_rt(item)
}

//#[doc(hidden)]
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
///     fn shl_assign(&mut self, other: Self){
///         self.int <<= other.int;
///     }
/// }
///
/// impl std::ops::ShrAssign for Custom {
///     fn shr_assign(&mut self, other: Self){
///         self.int >>= other.int;
///     }
/// }
///
/// impl std::ops::RemAssign for Custom {
///     fn rem_assign(&mut self, other: Self) {
///        self.int %= other.int;
///    }
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
///         AtomicArray::<Custom>::new(&world,3,Distribution::Block).block();
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
///     world.block_on( async move {  // we will just use the world as our future driver so we dont have to deal with cloning array
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
///         println!("(1) the updated array");
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

/// The macro used to create an new instance of a `TypedAmGroup` which is an Active Message Group that can only include AMs of a specific type (but this type can return data).
/// Data is returned in the same order as the AMs were added
/// (You can think of this as similar to `Vec<T>`)
/// This macro which expects two parameters, the first being the type (name) of the AM and the second being a reference to a lamellar team.
/// ```
/// use lamellar::active_messaging::prelude::*;
/// use lamellar::darc::prelude::*;
/// use std::sync::atomic::AtomicUsize;
///
/// #[AmData(Debug,Clone)]
/// struct ExampleAm {
///    cnt: Darc<AtomicUsize>,
/// }
///
/// #[lamellar::am]
/// impl LamellarAm for ExampleAm {
///     async fn exec(self) -> usize {
///         self.cnt.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
///     }
/// }
///
/// fn main() {
///     let world = lamellar::LamellarWorldBuilder::new().build();
///     let my_pe = world.my_pe();
///     let num_pes = world.num_pes();
///
///     if my_pe == 0 { // we only want to run this on PE0 for sake of illustration
///         let mut am_group = typed_am_group!{ExampleAm,&world};
///         let am = ExampleAm{cnt: Darc::new(&world, AtomicUsize::new(0)).block().unwrap()};
///         // add the AMs to the group
///         // we can specify individual PEs to execute on or all PEs
///         am_group.add_am_pe(0,am.clone());
///         am_group.add_am_all(am.clone());
///         am_group.add_am_pe(1,am.clone());
///         am_group.add_am_all(am.clone());
///
///         //execute and await the completion of all AMs in the group
///         let results = world.block_on(am_group.exec()); // we want to process the returned data
///         //we can index into the results
///         if let AmGroupResult::Pe(pe,val) = results.at(2) {
///             assert_eq!(pe, 1); //the third add_am_* call in the group was to execute on PE1
///             assert_eq!(*val, 1); // this was the second am to execute on PE1 so the fetched value is 1
///         }
///         //or we can iterate over the results
///         for res in results.iter() {
///             match res {
///                 AmGroupResult::Pe(pe,val) => { println!("{:?} from PE{:?}",val,pe)},
///                 AmGroupResult::All(val) => { println!("{:?} on all PEs",val)},
///             }
///         }
///     }
/// }
///```
/// Expected output on each PE1:
/// ```text
/// 0 from PE0
/// [1,0] on all PEs
/// 1 from PE1
/// [2,2] on all PEs
/// ```
/// ### Static Members
/// In the above code, the `ExampleAm` struct contains a member that is a `Darc` (Distributed Arc).
/// In order to properly calculate distributed reference counts Darcs implements specialized Serialize and Deserialize operations.
/// While, the cost to any single serialization/deserialization operation is small, doing this for every active message containing
/// a Darc can become expensive.
///
/// In certain cases Typed Am Groups can avoid the repeated serialization/deserialization of Darc members if the user guarantees
/// that every Active Message in the group is using a reference to the same Darc. In this case, we simply would only need
/// to serialize the Darc once for each PE it gets sent to.
///
/// This can be accomplished by using the [macro@AmData] attribute macro with the `static` keyword passed in as an argument as illustrated below:
/// ```
/// use lamellar::active_messaging::prelude::*;
/// use lamellar::darc::prelude::*;
/// use std::sync::Arc;
/// use std::sync::atomic::AtomicUsize;
///
/// #[AmData(Debug, Clone)]
/// struct ExampleAm {
///    #[AmGroup(static)]
///    cnt: Darc<AtomicUsize>,
/// }
///```
/// Other than the addition of `#[AmData(static)]` the rest of the code as the previous example would be the same.
#[proc_macro_error]
#[proc_macro]
pub fn typed_am_group(input: TokenStream) -> TokenStream {
    let am_group: AmGroups = syn::parse(input).unwrap();
    let am_type = am_group.am;
    let team = am_group.team;
    quote! {
        #am_type::create_am_group(#team)
    }
    .into()
}
