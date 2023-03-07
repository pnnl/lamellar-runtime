extern crate proc_macro;

mod parse;
mod replace;

mod array_ops;
mod array_reduce;

use crate::replace::LamellarDSLReplace;

use std::collections::HashMap;

use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_error::{abort,abort_call_site, proc_macro_error};
use quote::{quote, quote_spanned, ToTokens};
use syn::parse_macro_input;
use syn::spanned::Spanned;
use syn::visit_mut::VisitMut;

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
            syn::ImplItem::Method(ref item) => {
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
            syn::ImplItem::Method(ref item) => {
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
        syn::Stmt::Semi(expr, _semi) => match expr.clone() {
            syn::Expr::Return(expr) => Some(*(expr.expr.unwrap())),
            _ => None,
        },
        syn::Stmt::Expr(expr) => Some(expr.clone()),
        _ => {
            println!("something else!");
            None
        }
    };
    expr
}

fn replace_lamellar_dsl(mut stmt: syn::Stmt) -> syn::Stmt {
    LamellarDSLReplace.visit_stmt_mut(&mut stmt);
    stmt
}

enum AmType {
    NoReturn,
    ReturnData(syn::Type),
    ReturnAm(proc_macro2::TokenStream),
}

fn get_return_am_return_type(args: String) -> proc_macro2::TokenStream {
    let return_am = args
        .split("return_am")
        .collect::<Vec<&str>>()
        .last()
        .expect("error in lamellar::am argument")
        .trim_matches(&['=', ' ', '"'][..])
        .to_string();
    let mut return_type = "".to_string();
    if return_am.find("->") != None {
        let temp = return_am.split("->").collect::<Vec<&str>>();
        return_type = temp
            .last()
            .expect("error in lamellar::am argument")
            .trim()
            .to_string();
    }
    if return_type.len() > 0 {
        // let ident = syn::Ident::new(&return_type, Span::call_site());
        let ret_type: syn::Type = syn::parse_str(&return_type).expect("invalid type");
        quote! {#ret_type}
    } else {
        quote! {()}
    }
}

fn generate_am(input: syn::ItemImpl, local: bool, rt: bool, am_type: AmType) -> TokenStream {
    let name = type_name(&input.self_ty).expect("unable to find name");
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let generics = input.generics.clone();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    if !ty_generics.to_token_stream().is_empty() && !local {
        panic!("generics are not supported in lamellar active messages");
    }
    let mut exec_fn =
        get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
    let func_span = exec_fn.span();
    let (last_expr, vec_u8) = if let Some(stmt) = exec_fn.stmts.pop() {
        let last_stmt = replace_lamellar_dsl(stmt.clone());
        match am_type {
            AmType::NoReturn => (
                quote_spanned! {last_stmt.span()=>
                    #last_stmt
                    #lamellar::active_messaging::LamellarReturn::Unit
                },
                false,
            ),
            AmType::ReturnData(ref ret) => {
                let vec_u8 = match ret {
                    syn::Type::Array(a) => match &*a.elem {
                        syn::Type::Path(type_path)
                            if type_path.clone().into_token_stream().to_string() == "u8" =>
                        {
                            true
                        }
                        _ => false,
                    },
                    _ => false,
                };
                let temp = get_expr(&last_stmt)
                    .expect("failed to get exec return value (try removing the last \";\")");
                (quote_spanned! {last_stmt.span()=> #temp}, vec_u8)
            }
            AmType::ReturnAm(_) => {
                let temp = get_expr(&last_stmt)
                    .expect("failed to get exec return value (try removing the last \";\")");
                (quote_spanned! {last_stmt.span()=> #temp}, false)
            }
        }
    } else {
        (
            quote_spanned! {func_span=> #lamellar::active_messaging::LamellarReturn::Unit },
            false,
        )
    };

    let mut temp = quote_spanned! {func_span=>};
    let stmts = exec_fn.stmts;

    for stmt in stmts {
        let new_stmt = replace_lamellar_dsl(stmt.clone());
        temp.extend(quote_spanned! {stmt.span()=>
            #new_stmt
        });
    }

    let orig_name = syn::Ident::new(&name, Span::call_site());
    let orig_name_unpack = quote::format_ident!("{}_unpack", orig_name.clone());

    let ret_type = {
        match am_type {
            AmType::NoReturn => quote! {()},
            AmType::ReturnData(ref output) => quote! {#output},
            AmType::ReturnAm(ref output) => {
                quote! {#output}
            }
        }
    };
    let ret_struct = quote::format_ident!("{}Result", orig_name);
    let mut am = quote! {
        impl #impl_generics #lamellar::active_messaging::LocalAM for #orig_name #ty_generics #where_clause{
            type Output = #ret_type;
        }
    };
    if !local {
        am.extend(quote!{
            #[async_trait::async_trait]
            impl #impl_generics #lamellar::active_messaging::LamellarAM for #orig_name #ty_generics #where_clause {
                type Output = #ret_type;
                async fn exec(self) -> Self::Output{
                    panic!("this should never be called")
                }
            }

            impl  #impl_generics #lamellar::active_messaging::Serde for #orig_name #ty_generics #where_clause {}

            impl #impl_generics #lamellar::active_messaging::LamellarSerde for #orig_name #ty_generics #where_clause {
                fn serialized_size(&self)->usize{
                    // println!("serialized size: {:?}", #lamellar::serialized_size(self));
                    #lamellar::serialized_size(self,true)
                }
                fn serialize_into(&self,buf: &mut [u8]){
                    // println!("buf_len: {:?} serialized size: {:?}",buf.len(), #lamellar::serialized_size(self));
                    #lamellar::serialize_into(buf,self,true).unwrap();
                    // println!("buf_len: {:?} serialized size: {:?} buf: {:?}",buf.len(), #lamellar::serialized_size(self,true), buf);

                }
            }

            impl #impl_generics #lamellar::active_messaging::LamellarResultSerde for #orig_name #ty_generics #where_clause {
                fn serialized_result_size(&self,result: & Box<dyn std::any::Any + Sync + Send>)->usize{
                    let result  = result.downcast_ref::<#ret_type>().unwrap();
                    #lamellar::serialized_size(result,true)
                }
                fn serialize_result_into(&self,buf: &mut [u8],result: & Box<dyn std::any::Any + Sync + Send>){
                    let result  = result.downcast_ref::<#ret_type>().unwrap();
                    #lamellar::serialize_into(buf,result,true).unwrap();
                }
            }
        });
    }

    let ret_statement = match am_type {
        AmType::NoReturn => quote! {
            #last_expr
        },
        AmType::ReturnData(_) => {
            let remote_last_expr = if vec_u8 {
                quote! { ByteBuf::from(#last_expr) }
            } else {
                quote! { #last_expr }
            };
            if !local {
                quote! {
                    let ret = match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                        true => #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(#last_expr)),
                        false => #lamellar::active_messaging::LamellarReturn::RemoteData(std::sync::Arc::new (#ret_struct{
                            val: #remote_last_expr,
                            // _phantom: std::marker::PhantomData
                        })),
                    };
                    ret
                }
            } else {
                quote! {
                    #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(#last_expr))
                }
            }
        }
        AmType::ReturnAm(_) => {
            if !local {
                quote! {
                    let ret = match __local{
                        true => #lamellar::active_messaging::LamellarReturn::LocalAm(
                            std::sync::Arc::new (
                                #last_expr
                            )
                        ),
                        false => #lamellar::active_messaging::LamellarReturn::RemoteAm(
                            std::sync::Arc::new (
                                #last_expr
                            )
                        ),
                    };
                    ret
                }
            } else {
                quote! {
                    #lamellar::active_messaging::LamellarReturn::LocalAm(
                        std::sync::Arc::new (
                            #last_expr
                        )
                    )
                }
            }
        }
    };

    let my_name = quote! {stringify!(#orig_name)};

    let mut expanded = quote_spanned! {temp.span()=>
        impl #impl_generics #lamellar::active_messaging::LamellarActiveMessage for #orig_name #ty_generics #where_clause {
            fn exec(self: std::sync::Arc<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeam>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeam>) -> std::pin::Pin<Box<dyn std::future::Future<Output=#lamellar::active_messaging::LamellarReturn> + Send >>{
                Box::pin( async move {
                    #temp
                    #ret_statement
                }.instrument(#lamellar::tracing::trace_span!(#my_name))
            )

            }

            fn get_id(&self) -> &'static str{
                stringify!(#orig_name)//.to_string()
            }
        }

        #am
    };

    if let AmType::ReturnData(_) = am_type {
        // let generic_phantom = quote::format_ident!("std::marker::PhantomData{}", impl_generics);

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

        let the_ret_struct = if vec_u8 {
            quote! {
                #am_data_header
                struct #ret_struct #ty_generics #where_clause{
                    val: serde_bytes::ByteBuf,
                    // _phantom: std::marker::PhantomData<(#impl_generics)>,
                }
            }
        } else {
            quote! {
                #am_data_header
                struct #ret_struct #ty_generics #where_clause{
                    val: #ret_type,
                    // _phantom: std::marker::PhantomData<(#impl_generics)>,
                }
            }
        };

        expanded.extend(quote_spanned! {temp.span()=>
            #the_ret_struct
        });
        if !local {
            expanded.extend(quote_spanned! {temp.span()=>
                impl #impl_generics #lamellar::active_messaging::LamellarSerde for #ret_struct #ty_generics #where_clause {
                    fn serialized_size(&self)->usize{
                        #lamellar::serialized_size(&self.val,true)
                    }
                    fn serialize_into(&self,buf: &mut [u8]){
                        #lamellar::serialize_into(buf,&self.val,true).unwrap();
                    }
                }

                impl #impl_generics #lamellar::active_messaging::LamellarResultDarcSerde for #ret_struct #ty_generics #where_clause{}
            });
        }
    }

    if !local {
        expanded.extend( quote_spanned! {temp.span()=>
            impl #impl_generics #lamellar::active_messaging::RemoteActiveMessage for #orig_name #ty_generics #where_clause {
                fn as_local(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn #lamellar::active_messaging::LamellarActiveMessage + Send + Sync>{
                    self
                }
            }
            fn #orig_name_unpack #impl_generics (bytes: &[u8], cur_pe: Result<usize,#lamellar::IdError>) -> std::sync::Arc<dyn #lamellar::active_messaging::RemoteActiveMessage + Sync + Send>  {
                // println!("bytes len {:?} bytes {:?}",bytes.len(),bytes);
                let __lamellar_data: std::sync::Arc<#orig_name #ty_generics> = std::sync::Arc::new(#lamellar::deserialize(&bytes,true).unwrap());
                <#orig_name #ty_generics as #lamellar::active_messaging::DarcSerde>::des(&__lamellar_data,cur_pe);
                __lamellar_data
            }

            #lamellar::inventory::submit! {
                // #![crate = #lamellar]
                #lamellar::active_messaging::RegisteredAm{
                    exec: #orig_name_unpack,
                    name: stringify!(#orig_name)//.to_string()
                }
            }
        });
    }

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            use __lamellar::tracing::*;
            #expanded
        };
    };

    let rt_expanded = quote_spanned! {
        expanded.span()=>
        const _: () = {
            use tracing::*;
            #expanded
        };
    };

    if lamellar == "crate" {
        TokenStream::from(rt_expanded)
    } else {
        TokenStream::from(user_expanded)
    }
}

fn process_fields(
    args: syn::AttributeArgs,
    the_fields: &syn::Fields,
    crate_header: String,
    local: bool,
) -> (
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
) {
    let lamellar = quote::format_ident!("{}", crate_header.clone());
    let mut fields = quote! {};
    let mut ser = quote! {};
    let mut des = quote! {};

    for field in the_fields {
        if let syn::Type::Path(ref ty) = field.ty {
            if let Some(_seg) = ty.path.segments.first() {
                if !local {
                    let field_name = field.ident.clone();
                    fields.extend(quote_spanned! {field.span()=>
                        #field,
                    });
                    ser.extend(quote_spanned! {field.span()=>
                        (&self.#field_name).ser(num_pes,darcs);
                    });
                    des.extend(quote_spanned! {field.span()=>
                        (&self.#field_name).des(cur_pe);
                    });
                } else {
                    fields.extend(quote_spanned! {field.span()=>
                        #field,
                    });
                }
            }
        } else if let syn::Type::Tuple(ref ty) = field.ty {
            let field_name = field.ident.clone();
            let mut ind = 0;
            for elem in &ty.elems {
                if let syn::Type::Path(ref ty) = elem {
                    if let Some(_seg) = ty.path.segments.first() {
                        if !local {
                            let temp_ind = syn::Index {
                                index: ind,
                                span: field.span(),
                            };
                            ind += 1;
                            ser.extend(quote_spanned! {field.span()=>
                               ( &self.#field_name.#temp_ind).ser(num_pes,darcs);
                            });
                            des.extend(quote_spanned! {field.span()=>
                                (&self.#field_name.#temp_ind).des(cur_pe);
                            });
                        }
                    }
                }
            }
            fields.extend(quote_spanned! {field.span()=>
                #field,
            });
        } else if let syn::Type::Reference(ref _ty) = field.ty {
            if !local {
                panic!("references are not supported in Remote Active Messages");
            } else {
                fields.extend(quote_spanned! {field.span()=>
                    #field,
                });
            }
        } else {
            if !local {
                panic!("unsupported type in Remote Active Message {:?}", field.ty);
            }
            fields.extend(quote_spanned! {field.span()=>
                #field,
            });
        }
    }
    let my_ser: syn::Path = syn::parse(
        format!("{}::serde::Serialize", crate_header)
            .parse()
            .unwrap(),
    )
    .unwrap();
    let my_de: syn::Path = syn::parse(
        format!("{}::serde::Deserialize", crate_header)
            .parse()
            .unwrap(),
    )
    .unwrap();
    let mut impls = quote! {};
    if !local {
        impls.extend(quote! { #my_ser, #my_de, });
        // ser.extend(quote! { false });
    } else {
        ser.extend(quote! {panic!{"should not serialize data in LocalAM"} });
    }
    let mut trait_strs = HashMap::new();
    let mut attr_strs = HashMap::new();
    // let content;
    // if let Ok(temp)  = syn::parse::<syn::MetaList>(args.clone()).unwrap(){
    // // let temp = parse_macro_input!(args as  syn::AttributeArgs);
    //     println!("{temp:?}");
    // }
    // println!("{args:?}");
    // parenthesized!(content in temp);
    // for t in args.to_string().split(",") {
    for a in args {
        let t = a.to_token_stream().to_string();
        // let t = match a {
        //     syn::NestedMeta::Meta(m) => m.to_token_stream().to_string(),
        //     syn::NestedMeta::Lit(l) => l.to_token_stream().to_string(),
        // };
        if t.contains("Dist") {
            trait_strs
                .entry(String::from("Dist"))
                .or_insert(quote! {#lamellar::Dist});
            trait_strs
                .entry(String::from("Copy"))
                .or_insert(quote! {Copy});
            trait_strs
                .entry(String::from("Clone"))
                .or_insert(quote! {Clone});
        } else if t.contains("ArrayOps") {
            if t.contains("("){
                // println!("{:?} {:?} {:?}",t,t.find("("),t.find(")"));
                let attrs = &t[t.find("(").unwrap()..t.find(")").expect("missing \")\" in when declaring ArrayOp macro")+1];
                let attr_toks: proc_macro2::TokenStream = attrs.parse().unwrap();
                attr_strs.entry(String::from("array_ops")).or_insert(quote!{ #[array_ops #attr_toks]});
                trait_strs
                    .entry(String::from("ArrayOps"))
                    .or_insert(quote! {#lamellar::ArrayOps});
                trait_strs
                    .entry(String::from("Dist"))
                    .or_insert(quote! {#lamellar::Dist});
                trait_strs
                    .entry(String::from("Copy"))
                    .or_insert(quote! {Copy});
                trait_strs
                    .entry(String::from("Clone"))
                    .or_insert(quote! {Clone});
            }
            else{
                abort_call_site!("Trying to us the 'ArrayOp' macro but you must specify which array operations to derive, possible options include:
                            Arithmetic - requires std::Ops::{AddAssign,SubAssign,MulAssign,DivAssign,RemAssign} to be implemented on your data type
                            CompEx - compare exchange (epsilon) ops, requires std::cmp::{PartialEq,PartialOrd} to be implemented on your data type
                            Bitwise - requires std::Ops::{BitAndAssign,BitOrAssign,BitXorAssign} to be implemented on your data type
                            Shift - requires std::Ops::{ShlAssign,ShrAssign} to be implemented on you data type
                            All - convienence attribute for when all you wish to derive all the above operations (note all the required traits must be implemented for your type)
                            
                        Usage example: #[AmData(ArrayOps(Arithmetic,Shift))]");
            }
            
        } else if !t.contains("serde::Serialize")
            && !t.contains("serde::Deserialize")
            && t.trim().len() > 0
        {
            let temp = quote::format_ident!("{}", t.trim());
            trait_strs
                .entry(t.trim().to_owned())
                .or_insert(quote! {#temp});
        }
    }
    for t in trait_strs {
        // let temp = quote::format_ident!("{}", t);
        let temp = t.1;
        impls.extend(quote! {#temp, });
    }
    // println!();
    // println!("{:?}",impls);
    let traits = quote! { #[derive( #impls)] };
    let  serde_temp_2 = if crate_header != "crate" && !local {
        quote! {#[serde(crate = "lamellar::serde")]}
    } else {
        quote! {}
    };
    let mut attrs = quote!{#serde_temp_2};
    for (_,attr) in attr_strs{
        attrs = quote!{
            #attrs
            #attr
        };
    }

    (traits, attrs, fields, ser, des)
}

fn derive_am_data(
    input: TokenStream,
    args:  syn::AttributeArgs,
    crate_header: String,
    local: bool,
) -> TokenStream {
    let lamellar = quote::format_ident!("{}", crate_header.clone());
    let input: syn::Item = parse_macro_input!(input);
    let mut output = quote! {};

    if let syn::Item::Struct(data) = input {
        let name = &data.ident;
        let generics = data.generics.clone();
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let (traits, attrs, fields, ser, des) =
            process_fields(args, &data.fields, crate_header, local);
        // println!("{:?}",ser.to_string());
        let vis = data.vis.to_token_stream();
        let mut attributes = quote!();
        for attr in data.attrs {
            let tokens = attr.clone().to_token_stream();
            attributes.extend(quote! {#tokens});
        }
        output.extend(quote! {
            #attributes
            #traits
            #attrs
            #vis struct #name #impl_generics #where_clause{
                #fields
            }
            impl #impl_generics #lamellar::active_messaging::DarcSerde for #name #ty_generics #where_clause{
                fn ser (&self,  num_pes: usize, darcs: &mut Vec<#lamellar::active_messaging::RemotePtr>){
                    // println!("in outer ser");
                    #ser
                }
                fn des (&self,cur_pe: Result<usize, #lamellar::IdError>){
                    // println!("in outer des");
                    #des
                }
            }
        });
    }
    // else if let syn::Item::Enum(data) = input{ //todo handle enums....
    // //     let name = &data.ident;
    // //     let generics = data.generics.clone();
    // //     let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    // }
    TokenStream::from(output)
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
    let args = parse_macro_input!(args as syn::AttributeArgs);
    derive_am_data(input, args, "lamellar".to_string(), false)
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
    let args = parse_macro_input!(args as syn::AttributeArgs);
    derive_am_data(input, args, "lamellar".to_string(), true)
}

#[doc(hidden)]
#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmDataRT(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as syn::AttributeArgs);
    derive_am_data(input, args, "crate".to_string(), false)
}

#[doc(hidden)]
#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmLocalDataRT(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as syn::AttributeArgs);
    derive_am_data(input, args, "crate".to_string(), true)
}

fn parse_am(args: TokenStream, input: TokenStream, local: bool, rt: bool) -> TokenStream {
    let args = args.to_string();
    if args.len() > 0 {
        assert!(args.starts_with("return_am"), "#[lamellar::am] only accepts an (optional) argument of the form:
                                               #[lamellar::am(return_am = \"<am to exec upon return>\")]");
    }
    // println!("args: {:?}", args);
    let input: syn::Item = parse_macro_input!(input);

    let output = match input.clone() {
        syn::Item::Impl(input) => {
            let output = get_return_of_method("exec".to_string(), &input.items);
            match output {
                Some(output) => {
                    if args.len() > 0 {
                        let output = get_return_am_return_type(args);
                        generate_am(input, local, rt, AmType::ReturnAm(output))
                    } else {
                        generate_am(input, local, rt, AmType::ReturnData(output))
                    }
                }

                None => {
                    if args.len() > 0 {
                        panic!("no return type detected (try removing the last \";\"), but parameters passed into [#lamellar::am]
                        #[lamellar::am] only accepts an (optional) argument of the form:
                        #[lamellar::am(return_am = \"<am to exec upon return>\")]");
                    }
                    generate_am(input, local, rt, AmType::NoReturn)
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
    parse_am(args, input, false, false)
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
    parse_am(args, input, true, false)
}

#[doc(hidden)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, false, true)
}

#[doc(hidden)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am_local(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, true, true)
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

///
/// This derive macro is intended to be used with the [macro@AmData] attribute macro to enable a user defined type to be used in ActiveMessages
/// # Examples
///
/// ```
/// // this import includes everything we need
/// use lamellar::array::prelude::*;
///
///
/// #[lamellar::AmData(
///     // Lamellar traits
///     ArrayOps,      // needed to derive various LamellarArray Op traits
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
