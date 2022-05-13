extern crate proc_macro;

mod parse;
mod replace;

mod array_ops;
mod array_reduce;

use crate::replace::LamellarDSLReplace;

use std::collections::HashMap;

use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_error::proc_macro_error;
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
    let (last_expr,vec_u8) = if let Some(stmt) = exec_fn.stmts.pop() {
        let last_stmt = replace_lamellar_dsl(stmt.clone());
        match am_type {
            AmType::NoReturn => {
                (quote_spanned! {last_stmt.span()=>
                    #last_stmt
                    #lamellar::LamellarReturn::Unit
                },false)
            },
            AmType::ReturnData(ref ret) => {
                let vec_u8 = match ret{
                    syn::Type::Array(a) => {
                         match &*a.elem{
                            syn::Type::Path(type_path) if type_path.clone().into_token_stream().to_string() == "u8" => true,
                            _ => false,
                        }
                    },
                    _ => false,
                };
                let temp = get_expr(&last_stmt)
                    .expect("failed to get exec return value (try removing the last \";\")");
                (quote_spanned! {last_stmt.span()=> #temp},vec_u8)
            }
            AmType::ReturnAm(_) => {
                let temp = get_expr(&last_stmt)
                    .expect("failed to get exec return value (try removing the last \";\")");
                (quote_spanned! {last_stmt.span()=> #temp},false)
            }
        }
    } else {
        (quote_spanned! {func_span=> #lamellar::LamellarReturn::Unit },false)
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
        impl #impl_generics #lamellar::LocalAM for #orig_name #ty_generics #where_clause{
            type Output = #ret_type;
        }
    };
    if !local {
        am.extend(quote!{
            impl #impl_generics #lamellar::LamellarAM for #orig_name #ty_generics #where_clause {
                type Output = #ret_type;
            }

            impl  #impl_generics #lamellar::Serde for #orig_name #ty_generics #where_clause {}

            impl #impl_generics #lamellar::LamellarSerde for #orig_name #ty_generics #where_clause {
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

            impl #impl_generics #lamellar::LamellarResultSerde for #orig_name #ty_generics #where_clause {
                fn serialized_result_size(&self,result: &Box<dyn std::any::Any + Send + Sync>)->usize{
                    let result  = result.downcast_ref::<#ret_type>().unwrap();
                    #lamellar::serialized_size(result,true)
                }
                fn serialize_result_into(&self,buf: &mut [u8],result: &Box<dyn std::any::Any + Send + Sync>){
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
            let remote_last_expr = if vec_u8{
                quote!{ ByteBuf::from(#last_expr) }
            }
            else{
                quote!{ #last_expr }
            };
            quote! {                
                let ret = match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                    true => #lamellar::LamellarReturn::LocalData(Box::new(#last_expr)),
                    false => #lamellar::LamellarReturn::RemoteData(std::sync::Arc::new (#ret_struct{
                        val: #remote_last_expr,
                        _phantom: std::marker::PhantomData
                    })),
                };
                ret
            }
        }
        AmType::ReturnAm(_) => {
            quote! {
                let ret = match __local{
                    true => #lamellar::LamellarReturn::LocalAm(
                        std::sync::Arc::new (
                            #last_expr
                        )
                    ),
                    false => #lamellar::LamellarReturn::RemoteAm(
                        std::sync::Arc::new (
                            #last_expr
                        )
                    ),
                };
                ret
            }
        }
    };

    let mut expanded = quote_spanned! {temp.span()=>
        impl #impl_generics #lamellar::LamellarActiveMessage for #orig_name #ty_generics #where_clause {
            fn exec(self: std::sync::Arc<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeam>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeam>) -> std::pin::Pin<Box<dyn std::future::Future<Output=#lamellar::LamellarReturn> + Send>>{
                Box::pin( async move {
                #temp
                #ret_statement
                })

            }

            fn get_id(&self) -> String{
                stringify!(#orig_name).to_string()
            }
        }

        #am
    };

    if let AmType::ReturnData(_) = am_type {
        // let generic_phantom = quote::format_ident!("std::marker::PhantomData{}", impl_generics);

        let the_ret_struct = if vec_u8{
            quote!{
                struct #ret_struct #ty_generics #where_clause{
                    val: serde_bytes::ByteBuf,
                    // _phantom: std::marker::PhantomData<(#impl_generics)>,
                }
            }
        } else {
            quote!{
                struct #ret_struct #ty_generics #where_clause{
                    val: #ret_type,
                    // _phantom: std::marker::PhantomData<(#impl_generics)>,
                }
            }
        };
        expanded.extend(quote_spanned! {temp.span()=>
            #the_ret_struct

            impl #impl_generics #lamellar::LamellarSerde for #ret_struct #ty_generics #where_clause {
                fn serialized_size(&self)->usize{
                    #lamellar::serialized_size(&self.val,true)
                }
                fn serialize_into(&self,buf: &mut [u8]){
                    #lamellar::serialize_into(buf,&self.val,true).unwrap();
                }
            }
        });
    }

    if !local {
        expanded.extend( quote_spanned! {temp.span()=>
            impl #impl_generics #lamellar::RemoteActiveMessage for #orig_name #ty_generics #where_clause {}
            fn #orig_name_unpack #impl_generics (bytes: &[u8], cur_pe: Result<usize,#lamellar::IdError>) -> std::sync::Arc<dyn #lamellar::RemoteActiveMessage + Send + Sync>  {
                // println!("bytes len {:?} bytes {:?}",bytes.len(),bytes);
                let __lamellar_data: std::sync::Arc<#orig_name #ty_generics> = std::sync::Arc::new(#lamellar::deserialize(&bytes,true).unwrap());
                <#orig_name #ty_generics as #lamellar::DarcSerde>::des(&__lamellar_data,cur_pe);
                __lamellar_data
            }

            #lamellar::inventory::submit! {
                #![crate = #lamellar]
                #lamellar::RegisteredAm{
                    exec: #orig_name_unpack,
                    name: stringify!(#orig_name).to_string()
                }
            }
        });
    }

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            #expanded
        };
    };

    if lamellar == "crate" {
        TokenStream::from(expanded)
    } else {
        TokenStream::from(user_expanded)
    }
}

fn process_fields(args: TokenStream, the_fields: &syn::Fields, crate_header: String, local: bool) -> (proc_macro2::TokenStream,proc_macro2::TokenStream,proc_macro2::TokenStream,proc_macro2::TokenStream,proc_macro2::TokenStream){
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
                        (&self.#field_name).ser(num_pes,cur_pe);
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

                               ( &self.#field_name.#temp_ind).ser(num_pes,cur_pe);
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
        }else if let syn::Type::Reference(ref _ty) = field.ty {
            if !local {
                panic!("references are not supported in Remote Active Messages");
            } else {
                fields.extend(quote_spanned! {field.span()=>
                    #field,
                });
            }
        } else {
            if ! local{
                panic!("unsupported type in Remote Active Message {:?}",field.ty);
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
    } else {
        ser.extend(quote! {panic!{"should not serialize data in LocalAM"}});
    }
    let mut trait_strs = HashMap::new();
    for t in args.to_string().split(",") {
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
        } else if t.contains("ArithmeticOps") {
            trait_strs
                .entry(String::from("ArithmeticOps"))
                .or_insert(quote! {#lamellar::ArithmeticOps});
            trait_strs
                .entry(String::from("Dist"))
                .or_insert(quote! {#lamellar::Dist});
            trait_strs
                .entry(String::from("Copy"))
                .or_insert(quote! {Copy});
            trait_strs
                .entry(String::from("Clone"))
                .or_insert(quote! {Clone});
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
        // print!("{:?}, ",t.0);
        let temp = t.1;
        impls.extend(quote! {#temp, });
    }
    // println!();
    // println!("{:?}",impls);
    let traits = quote! { #[derive( #impls)] };
    let serde_temp_2 = if crate_header != "crate" && !local {
        quote! {#[serde(crate = "lamellar::serde")]}
    } else {
        quote! {}
    };
    
    (traits,serde_temp_2,fields,ser,des)
}

fn derive_am_data(
    input: TokenStream,
    args: TokenStream,
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

        let  (traits,serde_temp_2,fields,ser,des) = process_fields(args, &data.fields, crate_header, local);
        let vis = data.vis.to_token_stream();
        output.extend(quote! {
            #traits
            #serde_temp_2
            #vis struct #name #impl_generics #where_clause{
                #fields
            }
            impl #impl_generics #lamellar::DarcSerde for #name #ty_generics #where_clause{
                fn ser (&self,  num_pes: usize, cur_pe: Result<usize, #lamellar::IdError>) {
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

#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmData(args: TokenStream, input: TokenStream) -> TokenStream {
    derive_am_data(input, args, "lamellar".to_string(), false)
}

#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmLocalData(args: TokenStream, input: TokenStream) -> TokenStream {
    derive_am_data(input, args, "lamellar".to_string(), true)
}

#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmDataRT(args: TokenStream, input: TokenStream) -> TokenStream {
    derive_am_data(input, args, "crate".to_string(), false)
}

#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmLocalDataRT(args: TokenStream, input: TokenStream) -> TokenStream {
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

#[proc_macro_error]
#[proc_macro_attribute]
pub fn am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, false, false)
}

#[proc_macro_error]
#[proc_macro_attribute]
pub fn local_am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, true, false)
}

#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, false, true)
}

#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am_local(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args, input, true, true)
}

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

#[proc_macro_error]
#[proc_macro]
pub fn generate_reductions_for_type(item: TokenStream) -> TokenStream {
    array_reduce::__generate_reductions_for_type(item)
}

#[proc_macro_error]
#[proc_macro]
pub fn generate_reductions_for_type_rt(item: TokenStream) -> TokenStream {
    array_reduce::__generate_reductions_for_type_rt(item)
}


#[proc_macro_error]
#[proc_macro]
pub fn generate_ops_for_type(item: TokenStream) -> TokenStream {
    array_ops::__generate_ops_for_type(item)
}


#[proc_macro_error]
#[proc_macro]
pub fn generate_ops_for_type_rt(item: TokenStream) -> TokenStream {
    array_ops::__generate_ops_for_type_rt(item)
}

#[proc_macro_error]
#[proc_macro_derive(ArithmeticOps)]
pub fn derive_arrayops(input: TokenStream) -> TokenStream {
    array_ops:: __derive_arrayops(input)
}