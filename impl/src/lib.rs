extern crate proc_macro;

mod parse;
mod replace;

mod array_ops;
mod array_reduce;

// use crate::replace::LamellarDSLReplace;
// use crate::replace::SelfReplace;

use std::collections::HashMap;

use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_error::{abort, proc_macro_error};
use quote::{quote, quote_spanned, ToTokens};
use syn::parse_macro_input;
use syn::spanned::Spanned;
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

fn replace_lamellar_dsl_new(fn_block: syn::Block) -> syn::Block {
    let token_string = fn_block.to_token_stream().to_string();
    let split_lamellar = token_string.split("lamellar").collect::<Vec<_>>();
    let mut new_token_string = String::from(split_lamellar[0]);
    // let mut i = 1;
    // while i <  split_lamellar.len()-1
    //     let s = split_lamellar[i];
    for s in &split_lamellar[1..] {
        if s.trim_start().starts_with("::") {
            let temp = s.split_whitespace().collect::<Vec<_>>();
            if temp[1].starts_with("current_pe") {
                new_token_string += "__lamellar_current_pe";
                new_token_string += s.split_once("current_pe").unwrap().1;
            } else if temp[1].starts_with("num_pes") {
                new_token_string += "__lamellar_num_pes";
                new_token_string += s.split_once("num_pes").unwrap().1;
            } else if temp[1].starts_with("world") {
                new_token_string += "__lamellar_world";
                new_token_string += s.split_once("world").unwrap().1;
            } else if temp[1].starts_with("team") {
                new_token_string += "__lamellar_team";
                new_token_string += s.split_once("team").unwrap().1;
            }
        } else {
            new_token_string += "lamellar";
            new_token_string += s;
        }
    }
    // println!("{token_string}");
    // let mut new_token_string =token_string;
    // // for s in "lamellar ::"[]
    // if token_string.contains("lamellar ::"){
    //     new_token_string = new_token_string.replace("lamellar :: current_pe","__lamellar_current_pe");
    //     new_token_string = new_token_string.replace("lamellar :: num_pes","__lamellar_num_pes");
    //     new_token_string = new_token_string.replace("lamellar :: world","__lamellar_world");
    //     new_token_string = new_token_string.replace("lamellar :: team","__lamellar_team");
    // if token_string.contains("lamellar") {
    //     println!("{token_string}");
    //     println!("{new_token_string}");
    // }

    match syn::parse_str(&new_token_string) {
        Ok(fn_block) => fn_block,
        Err(_) => {
            println!("{token_string}");
            println!("{new_token_string}");
            panic!("uuhh ohh");
        }
    }
}

// fn replace_lamellar_dsl( stmt: syn::Stmt) -> syn::Stmt {
//     let token_string = stmt.to_token_stream().to_string();
//     let mut new_token_string = token_string.replace("lamellar::current_pe","__lamellar_current_pe");
//     new_token_string = new_token_string.replace("lamellar::num_pes","__lamellar_num_pes");
//     new_token_string = new_token_string.replace("lamellar::world","__lamellar_world");
//     new_token_string = new_token_string.replace("lamellar::team","__lamellar_team");

//     // println!("{token_string}");
//     // println!("{new_token_string}");
//     match syn::parse_str(&new_token_string){
//         Ok(stmt) => stmt,
//         Err(_) => {
//             println!("{token_string}");
//             println!("{new_token_string}");
//         panic!("uuhh ohh");
//         }
//     }
//     // stmt
// }

// fn replace_self(mut stmt: syn::Stmt, id: syn::Ident) -> syn::Stmt {
//     SelfReplace{id}.visit_stmt_mut(&mut stmt);
//     stmt
// }

fn replace_self_new(fn_block: syn::Block, id: &str) -> syn::Block {
    let token_string = fn_block.to_token_stream().to_string();
    let split_self = token_string.split("self").collect::<Vec<_>>();
    let mut new_token_string = String::from(split_self[0]);

    for s in &split_self[1..] {
        if s.trim_start().starts_with(".") {
            new_token_string += id;
        } else {
            new_token_string += "*";
            new_token_string += id;
        }
        new_token_string += s;
    }

    // let mut new_token_string = token_string.replace("self",id);
    // let mut new_token_string = token_string.replace("self",id);
    match syn::parse_str(&new_token_string) {
        Ok(fn_block) => fn_block,
        Err(_) => {
            println!("{token_string}");
            println!("{new_token_string}");
            panic!("uuhh ohh");
        }
    }
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

fn generate_am(
    input: syn::ItemImpl,
    local: bool,
    rt: bool,
    am_group: bool,
    am_type: AmType,
) -> TokenStream {
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
    // println!("{:?}",exec_fn.to_token_stream().to_string());
    exec_fn = replace_lamellar_dsl_new(exec_fn);
    let (last_expr, vec_u8) = if let Some(stmt) = exec_fn.stmts.pop() {
        // let last_stmt = replace_lamellar_dsl(stmt.clone());
        let last_stmt = stmt.clone();
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
        // let new_stmt = replace_lamellar_dsl(stmt.clone());
        temp.extend(quote_spanned! {stmt.span()=>
            #stmt
        });
    }

    let orig_name = syn::Ident::new(&name, Span::call_site());
    let orig_name_unpack = quote::format_ident!("{}_unpack", orig_name.clone());

    let mut is_return_am = false;

    let ret_type = {
        match am_type {
            AmType::NoReturn => quote! {()},
            AmType::ReturnData(ref output) => quote! {#output},
            AmType::ReturnAm(ref output) => {
                is_return_am = true;
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
                fn serialize(&self)->Vec<u8>{
                    #lamellar::serialize(self,true).unwrap()
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

    let (am_data_header, am_group_data_header, _am_group_header) = if rt {
        if !local {
            (
                quote! {#[lamellar_impl::AmDataRT]},
                quote! {#[lamellar_impl::AmGroupDataRT]},
                quote! {#[lamellar_impl::am_group_rt]},
            )
        } else {
            (
                quote! {#[lamellar_impl::AmLocalDataRT]},
                quote! {#[lamellar_impl::AmGroupDataRT]},
                quote! {#[lamellar_impl::am_group_local_rt]},
            )
        }
    } else {
        if !local {
            (
                quote! {#[#lamellar::AmData]},
                quote! {#[#lamellar::AmGroupData]},
                quote! {#[lamellar_impl::am_group]},
            )
        } else {
            (
                quote! {#[#lamellar::AmLocalData]},
                quote! {#[#lamellar::AmGroupData]},
                quote! {#[lamellar_impl::am_group_local]},
            )
        }
    };

    if let AmType::ReturnData(_) = am_type {
        // let generic_phantom = quote::format_ident!("std::marker::PhantomData{}", impl_generics);

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
                    fn serialize(&self)->Vec<u8>{
                        #lamellar::serialize(self,true).unwrap()
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

    if am_group && !is_return_am {
        let am_group_am_name = quote::format_ident!("{}GroupAm", orig_name);
        let am_group_am_name_local = quote::format_ident!("{}GroupAmLocal", orig_name);
        let am_group_name = quote::format_ident!("{}Group", orig_name);
        let am_group_return = quote::format_ident!("{}Result", am_group_name);
        let am_group_am_name_unpack = quote::format_ident!("{}_unpack", am_group_am_name);

        let mut temp = quote_spanned! {func_span=>};
        let mut exec_fn =
            get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
        exec_fn = replace_lamellar_dsl_new(exec_fn);
        exec_fn = replace_self_new(exec_fn, "am");
        let stmts = exec_fn.stmts;

        for stmt in stmts {
            // let new_stmt = replace_lamellar_dsl(stmt.clone());
            // let new_stmt = stmt.clone();
            // let new_stmt = replace_self(new_stmt,quote::format_ident!("am"));
            temp.extend(quote_spanned! {stmt.span()=>
                #stmt
            });
        }

        let ret_stmt = match am_type {
            AmType::NoReturn => {
                //quote!{#lamellar::active_messaging::LamellarReturn::Unit},
                if !local {
                    quote! {
                        match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                            true => #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(__res_vec)),
                            false => #lamellar::active_messaging::LamellarReturn::RemoteData(std::sync::Arc::new (#am_group_return{
                                vals: __res_vec,
                            })),
                        }
                    }
                } else {
                    quote! {
                        #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(__res_vec))
                    }
                }
            }
            AmType::ReturnData(ref _ret) => {
                if !local {
                    quote! {
                        match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                            true => #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(__res_vec)),
                            false => #lamellar::active_messaging::LamellarReturn::RemoteData(std::sync::Arc::new (#am_group_return{
                                vals: __res_vec,
                            })),
                        }
                    }
                } else {
                    quote! {
                        #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(__res_vec))
                    }
                }
            }
            AmType::ReturnAm(_) => {
                if !local {
                    quote! {
                         match __local{
                            true => #lamellar::active_messaging::LamellarReturn::LocalAm(std::sync::Arc::new (__res_vec)),
                            false => #lamellar::active_messaging::LamellarReturn::RemoteAm(std::sync::Arc::new (__res_vec)),
                        }
                    }
                } else {
                    quote! {
                        #lamellar::active_messaging::LamellarReturn::LocalAm(std::sync::Arc::new (__res_vec))
                    }
                }
            }
        };

        let am_group_return_struct = quote! {
            #am_data_header
            struct #am_group_return #ty_generics #where_clause{
                vals: Vec<#ret_type>,
            }
        };

        let local_generics = generics.clone();
        // if local_generics.params.len() == 0 {
        //     let lt = Token![<](Span::call_site());
        //     let gt = Token![>](Span::call_site());
        //     local_generics.lt_token = Some(lt);
        //     local_generics.gt_token = Some(gt);
        // }
        // local_generics.params.insert(0,syn::GenericParam::Lifetime(syn::LifetimeDef::new(syn::Lifetime::new("'am_group",Span::call_site()))));
        let (local_impl_generics, local_ty_generics, local_where_clause) =
            local_generics.split_for_impl();

        // println!("{:?} {:?} {:?}",local_impl_generics, local_ty_generics, local_where_clause);

        expanded.extend(quote!{
            impl #impl_generics  #orig_name #ty_generics #where_clause{
                fn create_am_group<U: Into<#lamellar::ArcLamellarTeam>>(team: U) -> #am_group_name  #impl_generics {
                    #am_group_name::new(team.into().team.clone())
                }
            }

            #am_data_header
            struct #am_group_am_name  #impl_generics #where_clause{
                #[Darc(iter)]
                ams: Vec<#orig_name #ty_generics>
            }

            #am_group_data_header
            struct #am_group_am_name_local  #local_impl_generics #local_where_clause{
                ams: Arc<Vec<#orig_name #ty_generics>>,
                si: usize,
                ei: usize,
            }

            impl #local_impl_generics lamellar::serde::Serialize for #am_group_am_name_local #local_ty_generics #local_where_clause {
                fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                    where
                        S: lamellar::serde::Serializer,
                    {
                        let ams = &self.ams[self.si..self.ei];
                        let mut seq = serializer.serialize_seq(Some(ams.len()))?;
                        for e in ams {
                            seq.serialize_element(e)?;
                        }
                        seq.end()
                    }
            }

            impl #impl_generics #lamellar::active_messaging::LocalAM for #am_group_am_name #ty_generics #where_clause{
                type Output = Vec<#ret_type>;
            }

            impl #local_impl_generics #lamellar::active_messaging::LocalAM for #am_group_am_name_local #local_ty_generics #local_where_clause {
                type Output = Vec<#ret_type>;
            }

            #[async_trait::async_trait]
            impl #impl_generics #lamellar::active_messaging::LamellarAM for #am_group_am_name #ty_generics #where_clause {
                type Output = Vec<#ret_type>;
                async fn exec(self) -> Self::Output{
                    panic!("this should never be called")
                }
            }

            #[async_trait::async_trait]
            impl #local_impl_generics #lamellar::active_messaging::LamellarAM for #am_group_am_name_local #local_ty_generics #local_where_clause {
                type Output = Vec<#ret_type>;
                async fn exec(self) -> Self::Output{
                    panic!("this should never be called")
                }
            }

            impl  #impl_generics #lamellar::active_messaging::Serde for #am_group_am_name #ty_generics #where_clause {}

            // impl  #local_impl_generics #lamellar::active_messaging::Serde for #am_group_am_name_local #local_ty_generics #local_where_clause {}

            impl #impl_generics #lamellar::active_messaging::LamellarSerde for #am_group_am_name #ty_generics #where_clause {
                fn serialized_size(&self)->usize{
                    #lamellar::serialized_size(self,true)
                }
                fn serialize_into(&self,buf: &mut [u8]){
                    #lamellar::serialize_into(buf,self,true).unwrap();
                }
                fn serialize(&self)->Vec<u8>{
                    #lamellar::serialize(self,true).unwrap()
                }
            }

            impl #local_impl_generics #lamellar::active_messaging::LamellarSerde for #am_group_am_name_local #local_ty_generics #local_where_clause {
                fn serialized_size(&self)->usize{
                    #lamellar::serialized_size(self,true)
                }
                fn serialize_into(&self,buf: &mut [u8]){
                    #lamellar::serialize_into(buf,self,true).unwrap();
                }
                fn serialize(&self)->Vec<u8>{
                    #lamellar::serialize(self,true).unwrap()
                }
            }

            impl #impl_generics #lamellar::active_messaging::LamellarResultSerde for #am_group_am_name #ty_generics #where_clause {
                fn serialized_result_size(&self,result: & Box<dyn std::any::Any + Sync + Send>)->usize{
                    let result  = result.downcast_ref::<Vec<#ret_type>>().unwrap();
                    #lamellar::serialized_size(result,true)
                }
                fn serialize_result_into(&self,buf: &mut [u8],result: & Box<dyn std::any::Any + Sync + Send>){
                    let result  = result.downcast_ref::<Vec<#ret_type>>().unwrap();
                    #lamellar::serialize_into(buf,result,true).unwrap();
                }
            }

            impl #local_impl_generics #lamellar::active_messaging::LamellarResultSerde for #am_group_am_name_local #local_ty_generics #local_where_clause {
                fn serialized_result_size(&self,result: & Box<dyn std::any::Any + Sync + Send>)->usize{
                    let result  = result.downcast_ref::<Vec<#ret_type>>().unwrap();
                    #lamellar::serialized_size(result,true)
                }
                fn serialize_result_into(&self,buf: &mut [u8],result: & Box<dyn std::any::Any + Sync + Send>){
                    let result  = result.downcast_ref::<Vec<#ret_type>>().unwrap();
                    #lamellar::serialize_into(buf,result,true).unwrap();
                }
            }

            impl #impl_generics #lamellar::active_messaging::LamellarActiveMessage for #am_group_am_name #ty_generics #where_clause {
                fn exec(self: std::sync::Arc<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeam>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeam>) -> std::pin::Pin<Box<dyn std::future::Future<Output=#lamellar::active_messaging::LamellarReturn> + Send >>{
                    Box::pin( async move {
                        // let __res_vec = self.ams.iter().map(|am| async {
                        //     #temp
                        // }).collect::<Vec<_>>();
                        let __res_vec = self.ams.iter().map(|am|  async  {
                            #temp
                        }).collect::<futures::stream::FuturesOrdered<_>>().collect::<Vec<_>>().await;
                        #ret_stmt
                    }.instrument(#lamellar::tracing::trace_span!(#my_name))
                )
                }
                fn get_id(&self) -> &'static str{
                    stringify!(#am_group_am_name)//.to_string()
                }
            }

            impl #local_impl_generics #lamellar::active_messaging::LamellarActiveMessage for #am_group_am_name_local #local_ty_generics #local_where_clause {
                fn exec(self: std::sync::Arc<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeam>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeam>) -> std::pin::Pin<Box<dyn std::future::Future<Output=#lamellar::active_messaging::LamellarReturn> + Send >>{
                    let ams = self.ams.clone();
                    Box::pin( async move {
                        // let __res_vec = futures::future::join_all(
                        let __res_vec = ams[self.si..self.ei].iter().map(|am|  async {
                            #temp
                        }).collect::<futures::stream::FuturesOrdered<_>>().collect::<Vec<_>>().await;
                        #ret_stmt
                    }.instrument(#lamellar::tracing::trace_span!(#my_name))
                    )
                }
                fn get_id(&self) -> &'static str{
                    stringify!(#am_group_am_name)//.to_string()
                }
            }
        });

        if !local {
            expanded.extend( quote_spanned! {input.span()=>
                impl #impl_generics #lamellar::active_messaging::RemoteActiveMessage for #am_group_am_name #ty_generics #where_clause {
                    fn as_local(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn #lamellar::active_messaging::LamellarActiveMessage + Send + Sync>{
                        self
                    }
                }
                impl #local_impl_generics #lamellar::active_messaging::RemoteActiveMessage for #am_group_am_name_local #local_ty_generics #local_where_clause {
                    fn as_local(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn #lamellar::active_messaging::LamellarActiveMessage + Send + Sync>{
                        self
                    }
                }
                fn #am_group_am_name_unpack #impl_generics (bytes: &[u8], cur_pe: Result<usize,#lamellar::IdError>) -> std::sync::Arc<dyn #lamellar::active_messaging::RemoteActiveMessage + Sync + Send>  {
                    // println!("bytes len {:?} bytes {:?}",bytes.len(),bytes);
                    let __lamellar_data: std::sync::Arc<#am_group_am_name #ty_generics> = std::sync::Arc::new(#lamellar::deserialize(&bytes,true).unwrap());
                    <#am_group_am_name #ty_generics as #lamellar::active_messaging::DarcSerde>::des(&__lamellar_data,cur_pe);
                    __lamellar_data
                }

                #lamellar::inventory::submit! {
                    // #![crate = #lamellar]
                    #lamellar::active_messaging::RegisteredAm{
                        exec: #am_group_am_name_unpack,
                        name: stringify!(#am_group_am_name)//.to_string()
                    }
                }

                #am_group_return_struct

                impl #impl_generics #lamellar::active_messaging::LamellarSerde for #am_group_return #ty_generics #where_clause {
                    fn serialized_size(&self)->usize{
                        #lamellar::serialized_size(&self.vals,true)
                    }
                    fn serialize_into(&self,buf: &mut [u8]){
                        #lamellar::serialize_into(buf,&self.vals,true).unwrap();
                    }
                    fn serialize(&self)->Vec<u8>{
                        #lamellar::serialize(self,true).unwrap()
                    }
                }

                impl #impl_generics #lamellar::active_messaging::LamellarResultDarcSerde for #am_group_return #ty_generics #where_clause{}
            });
        } else {
            expanded.extend(quote_spanned! {input.span()=>
                #am_group_return_struct
            });
        }

        expanded.extend( quote_spanned! {input.span()=>
             #[doc(hidden)]
            struct #am_group_name #impl_generics #where_clause{
                team: Arc<LamellarTeam>,
                cnt: usize,
                reqs: BTreeMap<usize,(Vec<usize>, Vec<#orig_name #ty_generics>,usize)>,
            }

            use #lamellar::active_messaging::LamellarSerde;
            impl #impl_generics #am_group_name #ty_generics #where_clause{
                #[allow(unused)]
                fn new(team: Arc<LamellarTeam>) -> #am_group_name #ty_generics{
                    #am_group_name {
                        team: team,
                        cnt: 0,
                        reqs: BTreeMap::new(),
                    }

                }
                #[allow(unused)]
                fn add_am_all(&mut self, am:  #orig_name #ty_generics)
                {

                    let req_queue = self.reqs.entry(self.team.num_pes()).or_insert((Vec::new(),Vec::new(),0));
                    req_queue.2 += am.serialized_size();
                    req_queue.0.push(self.cnt);
                    req_queue.1.push(am);
                    self.cnt+=1;
                }
                #[allow(unused)]
                fn add_am_pe(&mut self, pe: usize, am:  #orig_name #ty_generics)
                {
                    let req_queue = self.reqs.entry(pe).or_insert((Vec::new(),Vec::new(),0));
                    req_queue.2 += am.serialized_size();
                    req_queue.0.push(self.cnt);
                    req_queue.1.push(am);
                    self.cnt+=1;
                    // println!("cnt: {:?}",self.cnt);
                }
                #[allow(unused)]
                pub async fn exec(mut self) -> #lamellar::TypedAmGroupResult<#ret_type>{
                    // let timer = std::time::Instant::now();

                    let mut reqs: Vec<_> = Vec::new(); //: Vec<Pin<Box<dyn Future<Output = Vec<#ret_type>> + Send>>> = Vec::new();
                    let mut reqs_all: Vec<Pin<Box<dyn Future<Output = Vec<Vec<#ret_type>>> + Send>>> = Vec::new();

                    // let mut all_req = None;
                    let mut reqs_idx: BTreeMap<usize,(Vec<usize>,Vec<(usize,usize)>)> = BTreeMap::new();
                    let num_pes = self.team.num_pes();
                    // let num_workers = match std::env::var("LAMELLAR_THREADS") {
                    //     Ok(n) => n.parse::<usize>().unwrap(),
                    //     Err(_) => 4,
                    // };

                    // let num_workers = std::cmp::min(ams.len(),num_workers);
                    for (pe,the_ams) in self.reqs.iter_mut() {
                        let mut ams: Vec<#orig_name #ty_generics> = Vec::new();
                        let mut idx: Vec<usize> = Vec::new();
                        std::mem::swap(&mut ams,&mut the_ams.1);
                        std::mem::swap(&mut idx,&mut the_ams.0);

                        let ams = Arc::new(ams);
                        // let idx = Arc::new(idx);
                        let mut req_idx: Vec<(usize,usize)> = Vec::new();

                        // let num_elems = ams.len()/num_workers;
                        // // println!("num_elems {} num_workers {}",num_elems,num_workers);
                        // for i in 0..(num_workers-1){
                        //     // println!("si {} ei {}",i*num_elems,((i+1)*num_elems));
                        //     let tg_am = #am_group_am_name_local{ams: ams.clone(), si: i*num_elems, ei: ((i+1)*num_elems)};
                        //     if *pe == num_pes{
                        //         reqs_all.push(self.team.exec_am_group_all(tg_am));
                        //     }
                        //     else{
                        //         reqs.push(self.team.exec_am_group_pe(*pe,tg_am));
                        //     }
                        // }
                        // // println!("si {} ei {}",(num_workers-1)*num_elems,ams.len());
                        // let tg_am = #am_group_am_name_local{ams: ams.clone(), si: (num_workers-1)*num_elems, ei: ams.len()};
                        // if *pe == num_pes{
                        //     reqs_all.push(self.team.exec_am_group_all(tg_am));
                        // }
                        // else{
                        //     reqs.push(self.team.exec_am_group_pe(*pe,tg_am));
                        // }

                        if the_ams.2 > 1_000_000{
                            let num_reqs = (the_ams.2 / 1_000_000) + 1;
                            let req_size = the_ams.2/num_reqs;
                            let mut temp_size = 0;
                            let mut i = 0;
                            let mut start_i = 0;
                            let mut send = false;
                            while i < ams.len() {
                                let am_size = ams[i].serialized_size();
                                if temp_size + am_size < 100_000_000 { //hard size limit
                                    temp_size += am_size;
                                    i+=1;
                                    if temp_size > req_size{
                                        send = true
                                    }
                                }
                                else {
                                    send = true;
                                }
                                if send{
                                    let tg_am = #am_group_am_name_local{ams: ams.clone(), si: start_i, ei: i};
                                    if *pe == self.team.num_pes(){
                                        reqs_all.push(self.team.exec_am_group_all(tg_am));
                                    }
                                    else{
                                        reqs.push(#am_group_name::am_pe(self.team.clone(),tg_am,*pe));
                                    }
                                    req_idx.push((start_i, i));
                                    send = false;
                                    start_i = i;
                                    temp_size = 0;
                                }
                            }
                            if temp_size > 0 {
                                let tg_am = #am_group_am_name_local{ams: ams.clone(), si: start_i, ei: i};
                                if *pe == self.team.num_pes(){
                                    reqs_all.push(self.team.exec_am_group_all(tg_am));
                                }
                                else{
                                    reqs.push(#am_group_name::am_pe(self.team.clone(),tg_am,*pe));
                                }
                                req_idx.push((start_i, i));
                            }
                        }
                        else{
                            let tg_am = #am_group_am_name_local{ams: ams.clone(), si: 0, ei: ams.len()};
                            if *pe == self.team.num_pes(){
                                reqs_all.push(self.team.exec_am_group_all(tg_am));
                            }
                            else{
                                reqs.push(#am_group_name::am_pe(self.team.clone(),tg_am,*pe));
                            }
                            req_idx.push((0, ams.len()));
                        }
                        reqs_idx.insert(*pe,(idx,req_idx));
                        // reqs_pes.push(pe);
                    }

                    // println!("launch time: {:?} cnt: {:?} {:?} {:?}", timer.elapsed().as_secs_f64(),self.cnt,reqs.len(),reqs_all.len());

                    let pes = futures::future::join_all(reqs);
                    let all = futures::future::join_all(reqs_all);
                    let res = futures::join!(
                        pes,
                        all,
                        #am_group_name::create_idx(reqs_idx,self.cnt),
                    );

                    #lamellar::TypedAmGroupResult::new(
                        res.0, //  res.0, //vec![],// res.0.into_pe().unwrap(),
                        res.1, //vec![],//res.0.into_all().unwrap(),
                        res.2,
                        num_pes,
                    )
                }
                async fn am_pe(team: Arc<LamellarTeam>, tg_am: #am_group_am_name_local #local_ty_generics ,pe: usize) -> (usize,Vec<#ret_type>){
                    (pe, team.exec_am_group_pe(pe,tg_am).await)
                }
                async fn create_idx(reqs_idx: BTreeMap<usize,(Vec<usize>,Vec<(usize,usize)>)>,cnt: usize)->Vec<(usize,usize,usize)> {// Vec<(usize,usize,usize)> {//AmGroupReqs<#ret_type> {
                    let mut idx_map: Vec<(usize,usize,usize)> = Vec::new();
                    for _i in 0..cnt{
                        idx_map.push((0,0,0));
                    }
                    for (pe, (idx, req_idx)) in reqs_idx {
                        for (i, (si,ei)) in req_idx.iter().enumerate() {
                            for (j, req) in (*si..*ei).enumerate() {
                                idx_map[idx[req]] = (pe, i, j);
                            }
                        }
                    }
                    idx_map
                }
            }
        });
    }
    let am_group_mods = if am_group && !is_return_am {
        quote! {
            use __lamellar::active_messaging::prelude::*;
            use __lamellar::ser::SerializeSeq;
            use std::sync::Arc;
            use std::collections::BTreeMap;
            use futures::StreamExt;
            use std::pin::Pin;
            use std::future::Future;
        }
    } else {
        quote! {}
    };

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            use __lamellar::tracing::*;
            #am_group_mods
            #expanded
        };
    };

    let rt_expanded = quote_spanned! {
        expanded.span()=>
        const _: () = {
            use tracing::*;
            #am_group_mods
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
    the_fields: &mut syn::Fields,
    crate_header: String,
    local: bool,
    group: bool,
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

    // println!("process_fields: {local:?} {group:?} ");

    for field in the_fields {
        if let syn::Type::Path(ref ty) = field.ty {
            if let Some(_seg) = ty.path.segments.first() {
                if local {
                    fields.extend(quote_spanned! {field.span()=>
                        #field,
                    });
                } else if group {
                    // println!("process fields in group 0");
                    let field_name = field.ident.clone();
                    if &field_name.clone().unwrap().to_string() == "ams" {
                        ser.extend(quote_spanned! {field.span()=>
                            for e in (&self.ams[self.si..self.ei]).iter(){
                                e.ser(num_pes,darcs);
                            }
                        });
                        des.extend(quote_spanned! {field.span()=>
                            for e in (&self.ams[self.si..self.ei]).iter(){
                                e.des(cur_pe);
                            }
                        });
                    }
                    fields.extend(quote_spanned! {field.span()=>
                        #field,
                    });
                } else {
                    let field_name = field.ident.clone();
                    let darc_iter = field
                        .attrs
                        .iter()
                        .filter_map(|a| {
                            if a.to_token_stream().to_string().contains("#[Darc(iter)]") {
                                None
                            } else {
                                Some(a.clone())
                            }
                        })
                        .collect::<Vec<_>>();
                    if darc_iter.len() < field.attrs.len() {
                        ser.extend(quote_spanned! {field.span()=>
                            for e in (&self.#field_name).iter(){
                                e.ser(num_pes,darcs);
                            }
                        });
                        des.extend(quote_spanned! {field.span()=>
                            for e in (&self.#field_name).iter(){
                                e.des(cur_pe);
                            }
                        });
                    } else {
                        ser.extend(quote_spanned! {field.span()=>
                            (&self.#field_name).ser(num_pes,darcs);
                        });
                        des.extend(quote_spanned! {field.span()=>
                            (&self.#field_name).des(cur_pe);
                        });
                    }
                    field.attrs = darc_iter;
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
            if local {
                panic!("references are not supported in Remote Active Messages");
            } else if group {
                // println!("process fields in group 0");
                let field_name = field.ident.clone();
                if &field_name.clone().unwrap().to_string() == "ams" {
                    ser.extend(quote_spanned! {field.span()=>
                        for e in (&self.#field_name).iter(){
                            e.ser(num_pes,darcs);
                        }
                    });
                    des.extend(quote_spanned! {field.span()=>
                        for e in (&self.#field_name).iter(){
                            e.des(cur_pe);
                        }
                    });
                }
                fields.extend(quote_spanned! {field.span()=>
                    #field,
                });
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
    if local {
        ser.extend(quote! {panic!{"should not serialize data in LocalAM"} });
        // ser.extend(quote! { false });
    } else if group {
        // println!("process fields in group 1");
        // impls.extend(quote! { #my_ser });
    } else {
        impls.extend(quote! { #my_ser, #my_de, });
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
            if t.contains("(") {
                // println!("{:?} {:?} {:?}",t,t.find("("),t.find(")"));
                let attrs = &t[t.find("(").unwrap()
                    ..t.find(")")
                        .expect("missing \")\" in when declaring ArrayOp macro")
                        + 1];
                let attr_toks: proc_macro2::TokenStream = attrs.parse().unwrap();
                attr_strs
                    .entry(String::from("array_ops"))
                    .or_insert(quote! { #[array_ops #attr_toks]});
            }
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
            // else{
            //     abort_call_site!("Trying to us the 'ArrayOp' macro but you must specify which array operations to derive, possible options include:
            //                 Arithmetic - requires std::Ops::{AddAssign,SubAssign,MulAssign,DivAssign,RemAssign} to be implemented on your data type
            //                 CompEx - compare exchange (epsilon) ops, requires std::cmp::{PartialEq,PartialOrd} to be implemented on your data type
            //                 Bitwise - requires std::Ops::{BitAndAssign,BitOrAssign,BitXorAssign} to be implemented on your data type
            //                 Shift - requires std::Ops::{ShlAssign,ShrAssign} to be implemented on you data type
            //                 All - convienence attribute for when all you wish to derive all the above operations (note all the required traits must be implemented for your type)

            //             Usage example: #[AmData(ArrayOps(Arithmetic,Shift))]");
            // }
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
    let serde_temp_2 = if crate_header != "crate" && (!(local || group)) {
        // println!("process fields serde_temp_2");
        quote! {#[serde(crate = "lamellar::serde")]}
    } else {
        quote! {}
    };
    let mut attrs = quote! {#serde_temp_2};
    for (_, attr) in attr_strs {
        attrs = quote! {
            #attrs
            #attr
        };
    }

    (traits, attrs, fields, ser, des)
}

fn derive_am_data(
    input: TokenStream,
    args: syn::AttributeArgs,
    crate_header: String,
    local: bool,
    group: bool,
    rt: bool,
) -> TokenStream {
    let lamellar = quote::format_ident!("{}", crate_header.clone());
    let input: syn::Item = parse_macro_input!(input);
    let mut output = quote! {};

    if let syn::Item::Struct(mut data) = input {
        let name = &data.ident;
        let generics = data.generics.clone();
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        let (traits, attrs, fields, ser, des) =
            process_fields(args, &mut data.fields, crate_header, local, group);
        // println!("{:?}",ser.to_string());
        let vis = data.vis.to_token_stream();
        let mut attributes = quote!();
        for attr in data.attrs {
            let tokens = attr.clone().to_token_stream();
            attributes.extend(quote! {#tokens});
        }
        if rt {
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
        } else {
            output.extend(quote! {
                #attributes
                #traits
                #attrs
                #vis struct #name #impl_generics #where_clause{
                    #fields
                }
                const _: () = {
                    extern crate lamellar as __lamellar;
                    impl #impl_generics __lamellar::active_messaging::DarcSerde for #name #ty_generics #where_clause{
                        fn ser (&self,  num_pes: usize, darcs: &mut Vec<#lamellar::active_messaging::RemotePtr>){
                            // println!("in outer ser");
                            #ser
                        }
                        fn des (&self,cur_pe: Result<usize, #lamellar::IdError>){
                            // println!("in outer des");
                            #des
                        }
                    }
                };
            });
            // println!("{:?}",output.to_string());
        }
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
    derive_am_data(input, args, "lamellar".to_string(), false, false, false)
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
    derive_am_data(input, args, "lamellar".to_string(), true, false, false)
}

#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmGroupData(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as syn::AttributeArgs);
    derive_am_data(input, args, "lamellar".to_string(), false, true, false)
}

#[doc(hidden)]
#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmDataRT(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as syn::AttributeArgs);
    derive_am_data(input, args, "crate".to_string(), false, false, true)
}

#[doc(hidden)]
#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmLocalDataRT(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as syn::AttributeArgs);
    derive_am_data(input, args, "crate".to_string(), true, false, true)
}

fn parse_am(
    args: TokenStream,
    input: TokenStream,
    local: bool,
    rt: bool,
    am_group: bool,
) -> TokenStream {
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
                        generate_am(input, local, rt, am_group, AmType::ReturnAm(output))
                    } else {
                        generate_am(input, local, rt, am_group, AmType::ReturnData(output))
                    }
                }

                None => {
                    if args.len() > 0 {
                        panic!("no return type detected (try removing the last \";\"), but parameters passed into [#lamellar::am]
                        #[lamellar::am] only accepts an (optional) argument of the form:
                        #[lamellar::am(return_am = \"<am to exec upon return>\")]");
                    }
                    generate_am(input, local, rt, am_group, AmType::NoReturn)
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
///     ArrayOps(Arithmetic,CompEx,Shift), // needed to derive various LamellarArray Op traits (provided as a list)
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
