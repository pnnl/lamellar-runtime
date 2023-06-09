use crate::field_info::FieldInfo;
use crate::gen_am::*;
use crate::{get_expr, get_impl_method, replace_lamellar_dsl_new, type_name, AmType};
use crate::parse::FormatArgs;

use proc_macro2::Span;
use quote::{format_ident, quote, quote_spanned, ToTokens};
use syn::punctuated::Punctuated;
use syn::parse::Result;
use syn::spanned::Spanned;
fn replace_self_new(fn_block: syn::Block, id: &str) -> syn::Block {
    let mut block_token_string = String::from("{");
    for stmt in &fn_block.stmts {
        // println!("{:#?}", stmt);
        let token_string = stmt.to_token_stream().to_string();
        // let token_string = token_string.chars().filter(|&c| !c.is_whitespace()).collect::<String>();
        let split_self = token_string.split("self").collect::<Vec<_>>();
        let mut new_token_string = String::from(split_self[0]); //up to the first split
        for s in &split_self[1..] {
            // println!("{s}");
            if s.starts_with(".") {
                //the next character is a dot
                //get next non alphanumeric or underscore character
                for (idx, c) in s[1..].char_indices() {
                    if !c.is_alphanumeric() && c != '_' && c != ' ' {
                        //check if c is "("
                        if c == '(' {
                            //function call
                            // println!("function call {}", &format!("am{s}"));
                            new_token_string += &format!("am{s}");
                        } else {
                            // this was likely a field access
                            // println!("field access {}", &format!("self{}(am){}", &s[..=idx], &s[(idx+1)..]));
                            new_token_string +=
                                &format!("self{}(am){}", &s[..=idx], &s[(idx + 1)..]);
                        }
                        break;
                    }
                }
            } else {
                new_token_string += &format!("*am{s}");
            }
        }
        block_token_string += &new_token_string;
    }
    block_token_string += "}";
    match syn::parse_str(&block_token_string) {
        Ok(new_fn_block) => new_fn_block,
        Err(_) => {
            println!("{}", fn_block.to_token_stream().to_string());
            println!("{block_token_string}");
            panic!("uuhh ohh");
        }
    }
}

fn wrap_field_access(field: &syn::ExprField, id: &str) -> syn::Expr {
    let mut field = field.clone();
    field.base = Box::new(replace_expr_self(&field.base, id));
    syn::Expr::Paren(syn::ExprParen {
        attrs: vec![],
        paren_token: syn::token::Paren::default(),
        expr: Box::new(syn::Expr::Unary(syn::ExprUnary {
            attrs: vec![],
            op: syn::UnOp::Deref(syn::token::Star::default()),
            expr: Box::new(syn::Expr::Field(field)),
        })),
    })
}

fn replace_expr_self(expr: &syn::Expr, id: &str ) -> syn::Expr {
    let mut ret = expr.clone();
    match expr {
        syn::Expr::Call(call) => {
            let mut call = call.clone();
            call.func = Box::new(replace_expr_self(call.func.as_ref(), id));
            let new_args = call.args.iter().fold(Punctuated::new(),|mut acc, arg| {
                // acc.push(arg.clone());
                acc.push(replace_expr_self(arg, id));
                acc
            });
            call.args = new_args;
            ret = syn::Expr::Call(call);
        }
        syn::Expr::Field(field) => {
            let mut field = field.clone();
            field.base = Box::new(replace_expr_self(field.base.as_ref(), id));
            // if let syn::Expr::Path(path) = field.base.as_ref() {
            //     if let Some(ident) = path.path.get_ident() {
            //         if ident == "self" {
            //             ret = wrap_field_access(field,id);
            //         }
                    
            //     }
            // }
            ret = syn::Expr::Field(field);
        }
        syn::Expr::Index(index) => {
            let mut index = index.clone();
            index.expr = Box::new(replace_expr_self(index.expr.as_ref(), id));
            if let syn::Expr::Field(field) = index.index.as_ref() {
                if let syn::Expr::Path(path) = field.base.as_ref() {
                    if let Some(ident) = path.path.get_ident() {
                        if ident == "self" {
                            ret = wrap_field_access(field,id);
                        }
                    }
                }
            }
            ret = syn::Expr::Index(index);
        }
        syn::Expr::MethodCall(call) => {
            let mut call = call.clone();
            call.receiver = Box::new(replace_expr_self( call.receiver.as_ref(), id));
            let new_args = call.args.iter().fold(Punctuated::new(),|mut acc, arg| {
                acc.push(replace_expr_self(arg, id));
                acc
            });
            call.args = new_args;
            ret = syn::Expr::MethodCall(call);
        }
        syn::Expr::Path(path) => {
            if let Some(ident) = path.path.get_ident() {
                if ident == "self" {
                    let mut path = syn::ExprPath {
                        attrs: vec![],
                        qself: None,
                        path: syn::Path {
                            leading_colon: None,
                            segments: Punctuated::new(),
                        },
                    };
                    path.path.segments.push(syn::PathSegment {
                        ident: format_ident!("{}", id),
                        arguments: syn::PathArguments::None,
                    });
                    ret = syn::Expr::Path(path);
                }
            }
        }
        syn::Expr::Paren(paren) => {
            let mut paren = paren.clone();
            paren.expr = Box::new(replace_expr_self(paren.expr.as_ref(), id));
            ret = syn::Expr::Paren(paren);
        }

        _ => { println!("Unhandled expr {:?}", expr); }
    }
    ret
}

fn replace_self_new2(fn_block: syn::Block, id: &str) -> syn::Block {
    // let mut new_block = fn_block.clone();
    // for stmt in &mut new_block.stmts {
    //     match stmt {
    //         syn::Stmt::Local(local) => {
    //             // println!("{:#?}", local.init);
    //             if let Some(init) = &mut local.init {
    //                 init.expr = Box::new(replace_expr_self(init.expr.as_ref(), id));
    //             }
    //             // println!("{:#?}", local.init);
    //         }
    //         syn::Stmt::Item(item) => {
    //             panic!(
    //                 "Error unexpected item in lamellar am {}",
    //                 item.to_token_stream().to_string()
    //             );
    //         }
    //         syn::Stmt::Expr(expr, semi) => {
    //             // println!("{:#?}", expr);
    //             let new_expr = replace_expr_self(expr, id);
                
    //             *stmt = syn::Stmt::Expr(new_expr, semi.clone());
                
    //         }
    //         syn::Stmt::Macro(mac) => {
    //             // println!("{:#?}", mac);
    //             let mut args: Result<FormatArgs> = mac.mac.parse_body();
    //             // println!("{:#?}", args);
    //             if let Ok(args) = args {
    //                 let format_str = if args.format_string.to_token_stream().to_string().contains("self") {
    //                     args.format_string.to_token_stream().to_string().replace("self", id)
    //                 }
    //                 else {
    //                     args.format_string.to_token_stream().to_string()
    //                 };
    //                 let positional_args = args.positional_args.iter().map(|expr| {
    //                     // println!("{:#?}", expr);
    //                     let expr = replace_expr_self(expr, id);
    //                     // println!("new: {:#?}", expr);
    //                     expr
    //                 }).collect::<Vec<_>>();

    //                 let named_args = args.named_args.iter().map(|(name,expr)| {
    //                     // println!("{:#?}", expr);
    //                     let expr = replace_expr_self(expr, id);
    //                     // println!("new: {:#?}", expr);
    //                     expr
    //                 }).collect::<Vec<_>>();

    //                 let new_tokens = quote! {
    //                     #format_str, #(#positional_args),*, #(#named_args),*
    //                 };
    //                 // println!("{:?}", new_tokens.to_token_stream().to_string());
    //                 mac.mac.tokens=new_tokens;
    //             }
    //             else {
    //                 println!("Warning: support for non format like macros are experimental --  {:#?}", mac.to_token_stream().to_string());
    //                 let mac_string = mac.to_token_stream().to_string();
    //                 let mac_string = mac_string.replace("self", id);
    //                 let new_mac: syn::Macro = syn::parse_str(&mac_string).unwrap();
    //                 println!("{:#?}", new_mac.to_token_stream().to_string());
    //                 mac.mac = new_mac;
    //             }
    //         }
    //     }
    // }
    let token_string = fn_block.to_token_stream().to_string();
    let split_self = token_string.split("self").collect::<Vec<_>>();
    let mut new_token_string = String::from(split_self[0]);
    // let new_token_string = token_string.replace("self", id);
    for s in &split_self[1..] {
        if s.trim_start().starts_with(".") {
            // new_token_string += "*";
            new_token_string += id;
        } else {
            new_token_string += id;
        }
        new_token_string += s;
    }

    match syn::parse_str(&new_token_string) {
        Ok(fn_block) => fn_block,
        Err(_) => {
            println!("{token_string}");
            println!("{new_token_string}");
            panic!("uuhh ohh");
        }
    }
    // new_block
}

//maybe we simply create the original am using the lighter weight local clone of the static vars...
// this would allow us to directly use the original exec body... instead of using getters, and not being able to call methods
fn impl_am_group_remote_lamellar_active_message_trait(
    generics: &syn::Generics,
    am_group_am_name: &syn::Ident,
    am_group_body: &proc_macro2::TokenStream,
    ret_stmt: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let trace_name = quote! {stringify!(#am_group_am_name)};
    quote! {
        impl #impl_generics #lamellar::active_messaging::LamellarActiveMessage for #am_group_am_name #ty_generics #where_clause {
            fn exec(self: std::sync::Arc<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeam>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeam>) -> std::pin::Pin<Box<dyn std::future::Future<Output=#lamellar::active_messaging::LamellarReturn> + Send >>{
                Box::pin( async move {
                    // let all_time = std::time::Instant::now();
                    // let time_cnt1 = std::sync::atomic::AtomicUsize::new(0);
                    // let time_cnt2 = std::sync::atomic::AtomicUsize::new(0);
                    let __res_vec = self.ams.iter().map(|am|   {//async {
                        // let timer = std::time::Instant::now();
                        let am = self.as_orig_am(am);
                        // time_cnt1.fetch_add(timer.elapsed().as_micros().try_into().unwrap(), std::sync::atomic::Ordering::Relaxed);
                        // let timer = std::time::Instant::now();
                        #am_group_body
                        // time_cnt2.fetch_add(timer.elapsed().as_micros().try_into().unwrap(), std::sync::atomic::Ordering::Relaxed);
                    // }).collect::<#lamellar::futures::stream::FuturesOrdered<_>>().collect::<Vec<_>>().await;
                    }).collect::<Vec<_>>();
                    // println!("remote tid: {:?} all_time: {:?} time1: {:?} time2: {:?}",std::thread::current().id(), all_time.elapsed(), time_cnt1.load(std::sync::atomic::Ordering::Relaxed)as f32/1000000.0, time_cnt2.load(std::sync::atomic::Ordering::Relaxed)as f32/1000000.0);
                    #ret_stmt
                    }.instrument(#lamellar::tracing::trace_span!(#trace_name))
                )
            }
            fn get_id(&self) -> &'static str{
                stringify!(#am_group_am_name)//.to_string()
            }
        }
    }
}

fn impl_am_group_local_lamellar_active_message_trait(
    generics: &syn::Generics,
    am_group_am_name: &syn::Ident,
    am_group_body: &proc_macro2::TokenStream,
    ret_stmt: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let trace_name = quote! {stringify!(#am_group_am_name)};
    quote! {
        impl #impl_generics #lamellar::active_messaging::LamellarActiveMessage for #am_group_am_name #ty_generics #where_clause {
            fn exec(self: std::sync::Arc<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeam>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeam>) -> std::pin::Pin<Box<dyn std::future::Future<Output=#lamellar::active_messaging::LamellarReturn> + Send >>{
                let ams = self.ams.clone();
                Box::pin( async move {
                    // let all_time = std::time::Instant::now();
                    // let time_cnt1 = std::sync::atomic::AtomicUsize::new(0);
                    // let time_cnt2 = std::sync::atomic::AtomicUsize::new(0);
                    let __res_vec = ams[self.si..self.ei].iter().map(|am|  {//async {
                        // let timer = std::time::Instant::now();
                        let am = self.as_orig_am(am);
                        // time_cnt1.fetch_add(timer.elapsed().as_micros().try_into().unwrap(), std::sync::atomic::Ordering::Relaxed);
                        // let timer = std::time::Instant::now();
                        #am_group_body
                        // time_cnt2.fetch_add(timer.elapsed().as_micros().try_into().unwrap(), std::sync::atomic::Ordering::Relaxed);
                    // }).collect::<#lamellar::futures::stream::FuturesOrdered<_>>().collect::<Vec<_>>().await;
                    }).collect::<Vec<_>>();
                    // println!("local tid: {:?} all_time: {:?} time1: {:?} time2: {:?}",std::thread::current().id(), all_time.elapsed(), time_cnt1.load(std::sync::atomic::Ordering::Relaxed)as f32/1000000.0, time_cnt2.load(std::sync::atomic::Ordering::Relaxed)as f32/1000000.0);
                    #ret_stmt
                }.instrument(#lamellar::tracing::trace_span!(#trace_name))
                )
            }
            fn get_id(&self) -> &'static str{
                stringify!(#am_group_am_name)//.to_string()
            }
        }
    }
}

fn gen_am_group_remote_body(input: &syn::ItemImpl) -> proc_macro2::TokenStream {
    let mut exec_fn =
        get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
    exec_fn = replace_lamellar_dsl_new(exec_fn);
    exec_fn = replace_self_new2(exec_fn, "am"); // we wont change self to am if its referencing a static_var

    // println!("{}", exec_fn.to_token_stream().to_string());

    let mut am_body = quote_spanned! { exec_fn.span()=>};
    let stmts = exec_fn.stmts;

    for stmt in stmts {
        am_body.extend(quote_spanned! {stmt.span()=>
            #stmt
        });
    }
    am_body
}

fn gen_am_group_local_body(input: &syn::ItemImpl) -> proc_macro2::TokenStream {
    let mut exec_fn =
        get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
    exec_fn = replace_lamellar_dsl_new(exec_fn);
    exec_fn = replace_self_new2(exec_fn, "am"); // we wont change self to am if its referencing a static_var

    // println!("{}", exec_fn.to_token_stream().to_string());

    let mut am_body = quote_spanned! { exec_fn.span()=>};
    let stmts = exec_fn.stmts;

    for stmt in stmts {
        am_body.extend(quote_spanned! {stmt.span()=>
            #stmt
        });
    }
    am_body
}

fn gen_am_group_return_stmt(
    am_type: &AmType,
    am_group_return_name: &syn::Ident,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> proc_macro2::TokenStream {
    match am_type {
        AmType::NoReturn => {
            //quote!{#lamellar::active_messaging::LamellarReturn::Unit},
            if !local {
                quote! {
                    match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                        true => #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(())),
                        false => #lamellar::active_messaging::LamellarReturn::RemoteData(std::sync::Arc::new (#am_group_return_name{
                            val: (),
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
                        false => #lamellar::active_messaging::LamellarReturn::RemoteData(std::sync::Arc::new (#am_group_return_name{
                            val: __res_vec,
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
    }
}

fn impl_am_group_remote(
    generics: &syn::Generics,
    am_type: &AmType,
    inner_am_name: &syn::Ident,
    am_group_am_name: &syn::Ident,
    am_group_return_name: &syn::Ident,
    am_group_body: &proc_macro2::TokenStream,
    ret_type: &proc_macro2::TokenStream,
    ret_stmt: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let lamellar_active_message = impl_am_group_remote_lamellar_active_message_trait(
        generics,
        am_group_am_name,
        am_group_body,
        ret_stmt,
        lamellar,
    );
    let local_am = impl_local_am_trait(generics, am_group_am_name, ret_type, lamellar);
    let remote_trait_impls =
        impl_remote_traits(generics, am_group_am_name, ret_type, lamellar, false);

    quote! {
        #lamellar_active_message
        #local_am
        #remote_trait_impls
    }
}

fn impl_am_group_local_serialize(
    generics: &syn::Generics,
    name: &syn::Ident,
    am_group_am_name: &syn::Ident,
    am_group_am_name_local: &syn::Ident,
    static_fields: &FieldInfo,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let static_names = static_fields.names();
    let mut static_serializers = quote! {};
    for var in &static_names {
        static_serializers.extend(quote! {
            am_group.serialize_field(stringify!(#var), &self.#var)?;
        });
    }
    let num_members: syn::LitInt =
        syn::LitInt::new(&static_names.len().to_string(), Span::call_site());
    quote! {
        impl #impl_generics #lamellar::serde::Serialize for #am_group_am_name_local #ty_generics #where_clause {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: #lamellar::serde::Serializer,
            {


                let mut am_group = serializer.serialize_struct(stringify!(#am_group_am_name), #num_members)?;
                #static_serializers
                let ams = &self.ams[self.si..self.ei];
                am_group.serialize_field("ams",ams)?;
                am_group.end()
            }
        }
    }
}

fn impl_am_group_local(
    generics: &syn::Generics,
    am_type: &AmType,
    inner_am_name: &syn::Ident,
    am_group_am_name: &syn::Ident,
    am_group_am_name_local: &syn::Ident,
    am_group_return_name: &syn::Ident,
    am_group_body: &proc_macro2::TokenStream,
    ret_type: &proc_macro2::TokenStream,
    ret_stmt: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let lamellar_active_message = impl_am_group_local_lamellar_active_message_trait(
        generics,
        am_group_am_name_local,
        am_group_body,
        ret_stmt,
        lamellar,
    );
    let local_am = impl_local_am_trait(generics, am_group_am_name_local, ret_type, lamellar);
    let lamellar_am = impl_lamellar_am_trait(generics, am_group_am_name_local, ret_type, lamellar);
    let lamellar_serde = impl_lamellar_serde_trait(generics, am_group_am_name_local, lamellar);
    let lamellar_result_serde =
        impl_lamellar_result_serde_trait(generics, am_group_am_name_local, ret_type, lamellar);
    let remote_active_message =
        impl_remote_active_message_trait(generics, am_group_am_name_local, lamellar);
    let am_group_am_name_unpack = quote::format_ident!("{}_unpack", am_group_am_name.clone());

    quote! {
        #lamellar_active_message
        #local_am
        #lamellar_am
        #lamellar_serde
        #lamellar_result_serde
        #remote_active_message

        #lamellar::inventory::submit! {
            #lamellar::active_messaging::RegisteredAm{
                exec: #am_group_am_name_unpack,
                name: stringify!(#am_group_am_name_local)
            }
        }
    }
}

fn get_am_ref_name(name: &syn::Ident) -> syn::Ident {
    syn::Ident::new(&format!("{}Ref", name), name.span())
}

fn get_inner_am_name(name: &syn::Ident) -> syn::Ident {
    syn::Ident::new(&format!("{}InnerAm", name), name.span())
}

fn get_am_group_name(name: &syn::Ident) -> syn::Ident {
    syn::Ident::new(&format!("{}GroupAm", name), name.span())
}
fn get_am_group_local_name(name: &syn::Ident) -> syn::Ident {
    syn::Ident::new(&format!("{}GroupLocalAm", name), name.span())
}
fn get_am_group_local_ser_name(name: &syn::Ident) -> syn::Ident {
    syn::Ident::new(&format!("{}GroupLocalAmSer", name), name.span())
}
fn get_am_group_user_name(name: &syn::Ident) -> syn::Ident {
    syn::Ident::new(&format!("{}Group", name), name.span())
}
fn get_am_group_return_name(name: &syn::Ident) -> syn::Ident {
    syn::Ident::new(&format!("{}GroupReturn", name), name.span())
}

fn impl_am_group_user(
    generics: &syn::Generics,
    am_type: &AmType,
    am_name: &syn::Ident,
    am_group_inner_name: &syn::Ident,
    am_group_am_name_local: &syn::Ident,
    am_group_name_user: &syn::Ident,
    ret_type: &proc_macro2::TokenStream,
    inner_ret_type: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let typed_am_group_result_type = match am_type {
        AmType::NoReturn => quote! {
            #lamellar::TypedAmGroupResult::Unit(
                #lamellar::TypedAmGroupUnitResult::new(
                    res.2,
                    num_pes
                )
            )
        },
        AmType::ReturnData(ref output) => quote! {
            #lamellar::TypedAmGroupResult::Val(
                #lamellar::TypedAmGroupValResult::new(
                    res.0, //  res.0, //vec![],// res.0.into_pe().unwrap(),
                    res.1, //vec![],//res.0.into_all().unwrap(),
                    res.2,
                    num_pes,
                )
            )
        },
        AmType::ReturnAm(ref output) => quote! {
            #lamellar::TypedAmGroupResult::Val(
                #lamellar::TypedAmGroupValResult::new(
                    res.0, //  res.0, //vec![],// res.0.into_pe().unwrap(),
                    res.1, //vec![],//res.0.into_all().unwrap(),
                    res.2,
                    num_pes,
                )
            )
        },
    };

    // println!("{:?}", typed_am_group_result_type.to_string());

    quote! {
        #[doc(hidden)]
        struct #am_group_name_user #impl_generics #where_clause{
            team: std::sync::Arc<#lamellar::LamellarTeam>,
            cnt: usize,
            reqs: std::collections::BTreeMap<usize,(Vec<usize>, Vec<#am_group_inner_name #ty_generics>,usize)>,
            local_am: Option<#am_group_am_name_local #ty_generics>,
        }

    //  use #lamellar::active_messaging::LamellarSerde;
        impl #impl_generics #am_group_name_user #ty_generics #where_clause{
            #[allow(unused)]
            fn new(team: std::sync::Arc<#lamellar::LamellarTeam>) -> #am_group_name_user #ty_generics{
                #am_group_name_user {
                    team: team,
                    cnt: 0,
                    reqs: std::collections::BTreeMap::new(),
                    local_am: None
                }
            }
            #[allow(unused)]
            fn add_am_all(&mut self, am:  #am_name #ty_generics)
            {
                let req_queue = self.reqs.entry(self.team.num_pes()).or_insert_with(|| {
                    self.local_am = Some(am.as_am_group_local());
                    let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
                        Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
                        Err(_) => 10000,                      //+ 1 to account for main thread
                    };
                    (Vec::with_capacity(num_per_batch),Vec::with_capacity(num_per_batch),1000000)
                });
                // req_queue.2 += am.serialized_size();
                req_queue.0.push(self.cnt);
                req_queue.1.push(am.into_am_group_inner());
                self.cnt+=1;
            }
            #[allow(unused)]
            fn add_am_pe(&mut self, pe: usize, am:  #am_name #ty_generics)
            {
                // if self.local_am.is_none() {
                //     self.local_am = Some(am.as_am_group_local());
                // }
                let req_queue = self.reqs.entry(pe).or_insert_with(|| {
                    self.local_am = Some(am.as_am_group_local());
                    let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
                        Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
                        Err(_) => 10000,                      //+ 1 to account for main thread
                    };
                    (Vec::with_capacity(num_per_batch),Vec::with_capacity(num_per_batch),1000000)
                });
                // req_queue.2 += am.serialized_size();
                req_queue.0.push(self.cnt);
                req_queue.1.push(am.into_am_group_inner());
                self.cnt+=1;
                // println!("cnt: {:?}",self.cnt);
            }
            #[allow(unused)]
            pub async fn exec(mut self) -> #lamellar::TypedAmGroupResult<#inner_ret_type>{
                let timer = std::time::Instant::now();

                let mut reqs: Vec<_> = Vec::new();
                let mut reqs_all: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = Vec<#ret_type>> + Send>>> = Vec::new();
                let mut reqs_idx: std::collections::BTreeMap<usize,(Vec<usize>,Vec<(usize,usize)>)> = std::collections::BTreeMap::new();
                let num_pes = self.team.num_pes();

                let local_am = self.local_am.as_ref().expect("local am should exist");

                for (pe,the_ams) in self.reqs.iter_mut() {
                    let mut ams: Vec<#am_group_inner_name #ty_generics> = Vec::new();
                    let mut idx: Vec<usize> = Vec::new();
                    std::mem::swap(&mut ams,&mut the_ams.1);
                    std::mem::swap(&mut idx,&mut the_ams.0);

                    let ams = std::sync::Arc::new(ams);
                    // let idx = std::sync::Arc::new(idx);
                    let mut req_idx: Vec<(usize,usize)> = Vec::new();

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
                                let mut tg_am = local_am.clone();
                                tg_am.ams = ams.clone();
                                tg_am.si = start_i;
                                tg_am.ei = i;
                                // let tg_am = #am_group_am_name_local{ams: ams.clone(), si: start_i, ei: i};
                                if *pe == self.team.num_pes(){
                                    reqs_all.push(self.team.exec_am_group_all(tg_am));
                                }
                                else{
                                    reqs.push(#am_group_name_user::am_pe(self.team.clone(),tg_am,*pe));
                                }
                                req_idx.push((start_i, i));
                                send = false;
                                start_i = i;
                                temp_size = 0;
                            }
                        }
                        if temp_size > 0 {
                            let mut tg_am =  local_am.clone();
                            tg_am.ams = ams.clone();
                            tg_am.si = start_i;
                            tg_am.ei = i;
                            // let tg_am = #am_group_am_name_local{ams: ams.clone(), si: start_i, ei: i};
                            if *pe == self.team.num_pes(){
                                reqs_all.push(self.team.exec_am_group_all(tg_am));
                            }
                            else{
                                reqs.push(#am_group_name_user::am_pe(self.team.clone(),tg_am,*pe));
                            }
                            req_idx.push((start_i, i));
                        }
                    }
                    else{
                        let mut tg_am =  local_am.clone();
                        tg_am.ams = ams.clone();
                        tg_am.si = 0;
                        tg_am.ei = ams.len();
                        // let tg_am = #am_group_am_name_local{ams: ams.clone(), si: 0, ei: ams.len()};
                        if *pe == self.team.num_pes(){
                            reqs_all.push(self.team.exec_am_group_all(tg_am));
                        }
                        else{
                            reqs.push(#am_group_name_user::am_pe(self.team.clone(),tg_am,*pe));
                        }
                        req_idx.push((0, ams.len()));
                    }
                    reqs_idx.insert(*pe,(idx,req_idx));
                    // reqs_pes.push(pe);
                }

                println!("launch time: {:?} cnt: {:?} {:?} {:?}", timer.elapsed().as_secs_f64(),self.cnt,reqs.len(),reqs_all.len());

                let pes = #lamellar::futures::future::join_all(reqs);
                let all = #lamellar::futures::future::join_all(reqs_all);
                let res = #lamellar::futures::join!(
                    pes,
                    all,
                    #am_group_name_user::create_idx(reqs_idx,self.cnt),
                );


                #typed_am_group_result_type
            }

            #[allow(unused)]
            pub async fn exec2(mut self) -> #lamellar::TypedAmGroupResult<#inner_ret_type>{
                let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
                    Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
                    Err(_) => 10000,                      //+ 1 to account for main thread
                };
                let timer = std::time::Instant::now();

                let mut reqs: Vec<_> = Vec::new();
                let mut reqs_all: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = Vec<#ret_type>> + Send>>> = Vec::new();
                let mut reqs_idx: std::collections::BTreeMap<usize,(Vec<usize>,Vec<(usize,usize)>)> = std::collections::BTreeMap::new();
                let num_pes = self.team.num_pes();

                let local_am = self.local_am.as_ref().expect("local am should exist");

                for (pe,the_ams) in self.reqs.iter_mut() {
                    let mut ams: Vec<#am_group_inner_name #ty_generics> = Vec::new();
                    let mut idx: Vec<usize> = Vec::new();
                    std::mem::swap(&mut ams,&mut the_ams.1);
                    std::mem::swap(&mut idx,&mut the_ams.0);

                    let ams = std::sync::Arc::new(ams);
                    // let idx = std::sync::Arc::new(idx);
                    let mut req_idx: Vec<(usize,usize)> = Vec::new();

                    for i in (0..ams.len()).step_by(num_per_batch){
                        let end_i = std::cmp::min(i+num_per_batch,ams.len());

                        let mut tg_am =  local_am.clone();
                        tg_am.ams = ams.clone();
                        tg_am.si = i;
                        tg_am.ei = end_i;
                        // let tg_am = #am_group_am_name_local{ams: ams.clone(), si: start_i, ei: i};
                        if *pe == self.team.num_pes(){
                            reqs_all.push(self.team.exec_am_group_all(tg_am));
                        }
                        else{
                            reqs.push(#am_group_name_user::am_pe(self.team.clone(),tg_am,*pe));
                        }
                        req_idx.push((i, end_i));

                    }
                    reqs_idx.insert(*pe,(idx,req_idx));
                    // reqs_pes.push(pe);
                }

                println!("launch time: {:?} cnt: {:?} {:?} {:?}", timer.elapsed().as_secs_f64(),self.cnt,reqs.len(),reqs_all.len());

                let pes = #lamellar::futures::future::join_all(reqs);
                let all = #lamellar::futures::future::join_all(reqs_all);
                let res = #lamellar::futures::join!(
                    pes,
                    all,
                    #am_group_name_user::create_idx(reqs_idx,self.cnt),
                );


                #typed_am_group_result_type
            }
            async fn am_pe(team: std::sync::Arc<#lamellar::LamellarTeam>, tg_am: #am_group_am_name_local #ty_generics ,pe: usize) -> (usize,#ret_type){
                (pe, team.exec_am_group_pe(pe,tg_am).await)
            }
            async fn create_idx(reqs_idx: std::collections::BTreeMap<usize,(Vec<usize>,Vec<(usize,usize)>)>,cnt: usize)->Vec<(usize,usize,usize)> {// Vec<(usize,usize,usize)> {//AmGroupReqs<#ret_type> {
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
    }
}

pub(crate) fn generate_am_group(
    input: &syn::ItemImpl,
    local: bool,
    am_type: AmType,
    lamellar: &proc_macro2::TokenStream,
    am_data_header: &proc_macro2::TokenStream,
) -> proc_macro::TokenStream {
    let name = type_name(&input.self_ty).expect("unable to find name");
    let orig_name = syn::Ident::new(&name, Span::call_site());
    let am_group_am_name = get_am_group_name(&orig_name);
    let am_group_am_name_inner = get_inner_am_name(&orig_name);
    let am_group_am_name_local = get_am_group_local_name(&orig_name);
    let am_group_user_name = get_am_group_user_name(&orig_name);
    let am_group_return_name = get_am_group_return_name(&orig_name);
    let inner_am_name = get_inner_am_name(&orig_name);

    let am_group_am_name_unpack = quote::format_ident!("{}_unpack", am_group_am_name);

    let generics = input.generics.clone();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let (ret_type, inner_ret_type) = {
        match am_type {
            AmType::NoReturn => (quote! {()}, quote! {()}),
            AmType::ReturnData(ref output) => (quote! {Vec<#output>}, quote! {#output}),
            AmType::ReturnAm(ref output) => (quote! {Vec<#output>}, quote! {#output}),
        }
    };

    let am_group_remote_body = gen_am_group_remote_body(&input);
    let am_group_local_body = gen_am_group_local_body(&input);

    let ret_stmt = gen_am_group_return_stmt(&am_type, &am_group_return_name, lamellar, false);

    let am_group_remote = impl_am_group_remote(
        &generics,
        &am_type,
        &inner_am_name,
        &am_group_am_name,
        &am_group_return_name,
        &am_group_remote_body,
        &ret_type,
        &ret_stmt,
        &lamellar,
    );
    let am_group_local = impl_am_group_local(
        &generics,
        &am_type,
        &inner_am_name,
        &am_group_am_name,
        &am_group_am_name_local,
        &am_group_return_name,
        &am_group_local_body,
        &ret_type,
        &ret_stmt,
        &lamellar,
    );

    let am_group_return = impl_return_struct(
        &generics,
        &am_data_header,
        &am_group_return_name,
        &ret_type,
        &lamellar,
        false,
        local,
    );

    let am_group_user = impl_am_group_user(
        &generics,
        &am_type,
        &orig_name,
        &am_group_am_name_inner,
        &am_group_am_name_local,
        &am_group_user_name,
        &ret_type,
        &inner_ret_type,
        &lamellar,
    );

    let mut expanded = quote! {
        impl #impl_generics #orig_name #ty_generics #where_clause{
            fn create_am_group<U: Into<#lamellar::ArcLamellarTeam>>(team: U) -> #am_group_user_name #impl_generics {
                #am_group_user_name::new(team.into().team.clone())
            }
        }

        #am_group_remote

        #am_group_local

        #am_group_return

        #am_group_user
    };

    let am_group_mods = quote! {
        use __lamellar::active_messaging::prelude::*;
        use __lamellar::futures::StreamExt;
        use std::sync::Arc;
        use std::collections::BTreeMap;
        use std::pin::Pin;
        use std::future::Future;
    };

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

    if lamellar.to_string() == "crate" {
        proc_macro::TokenStream::from(rt_expanded)
    } else {
        proc_macro::TokenStream::from(user_expanded)
    }
}

fn create_am_ref(
    generics: &syn::Generics,
    attributes: &proc_macro2::TokenStream,
    traits: &proc_macro2::TokenStream,
    attrs: &proc_macro2::TokenStream,
    vis: &proc_macro2::TokenStream,
    name: &syn::Ident,
    fields: &FieldInfo,
    static_fields: &FieldInfo,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> proc_macro2::TokenStream {
    let mut generics = generics.clone();
    if static_fields.len() > 0 {
        generics
            .params
            .push(syn::GenericParam::Lifetime(syn::LifetimeParam {
                attrs: Vec::new(),
                lifetime: syn::Lifetime::new("'a", Span::call_site()),
                colon_token: None,
                bounds: Punctuated::new(),
            }));
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let name_ref = get_am_ref_name(name);

    let mut field_defs = quote! {};
    for (f, t) in fields.names().iter().zip(fields.types().iter()) {
        field_defs.extend(quote! { #f:  #t,})
    }
    let mut static_field_defs = quote! {};
    for (f, t) in static_fields
        .names()
        .iter()
        .zip(static_fields.types().iter())
    {
        static_field_defs.extend(quote! { #f: &'a #t,})
    }

    let mut derives = quote! {};
    if traits.to_string().contains("Debug") || attrs.to_string().contains("Debug") {
        derives.extend(quote! {Debug});
    }
    quote! {
        #[derive(#derives)]
        #vis struct #name_ref #impl_generics #where_clause{
            #field_defs
            #static_field_defs
        }
    }
}

fn create_am_group_inner(
    generics: &syn::Generics,
    attributes: &proc_macro2::TokenStream,
    traits: &proc_macro2::TokenStream,
    attrs: &proc_macro2::TokenStream,
    vis: &proc_macro2::TokenStream,
    name: &syn::Ident,
    fields: &FieldInfo,
    static_fields: &FieldInfo,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let (_impl_generics, ty_generics, _where_clause) = generics.split_for_impl();
    let am_group_inner_name = get_inner_am_name(name);
    let (inner_am, inner_am_traits) = create_am_struct(
        generics,
        attributes,
        traits,
        attrs,
        vis,
        &am_group_inner_name,
        &fields.to_token_stream(),
        &fields.ser(),
        &fields.des(),
        lamellar,
        local,
    );
    let lamellar_serde = impl_lamellar_serde_trait(generics, &am_group_inner_name, lamellar);
    (
        inner_am,
        quote! {
            #lamellar_serde
            #inner_am_traits
        },
    )
}

fn create_as_orig_am_ref(
    generics: &syn::Generics,
    name: &syn::Ident,
    fields: &FieldInfo,
    static_fields: &FieldInfo,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let name_ref = get_am_ref_name(name);
    let am_group_inner_name = get_inner_am_name(name);
    let mut field_intos_am = quote! {};
    for fname in fields.names() {
        field_intos_am.extend(quote! {
            #fname: am.#fname.clone(),
        });
    }

    let mut static_intos = quote! {};
    for fname in static_fields.names() {
        static_intos.extend(quote! {
            #fname: &self.#fname,
        });
    }

    let mut ref_generics = generics.clone();
    let mut ref_lifetime = quote!{};
    let mut ref_lifetime_def = quote!{};
    if static_fields.len() > 0 {
        ref_generics
        .params
        .push(syn::GenericParam::Lifetime(syn::LifetimeParam {
            attrs: Vec::new(),
            lifetime: syn::Lifetime::new("'_", Span::call_site()),
            colon_token: None,
            bounds: Punctuated::new(),
        }));
        ref_lifetime = quote!{'a};
        ref_lifetime_def = quote!{<#ref_lifetime>};
    }
    
    let (_, ty_generics, _) = generics.split_for_impl();
    let (_, ref_ty_generics, _) = ref_generics.split_for_impl();
    quote! {
        fn as_orig_am_ref #ref_lifetime_def (&#ref_lifetime self, am: &#am_group_inner_name #ty_generics) -> #name_ref #ref_ty_generics {
            #name_ref{
                #static_intos
                #field_intos_am
            }
        }
    }
}

fn create_as_orig_am(
    generics: &syn::Generics,
    name: &syn::Ident,
    fields: &FieldInfo,
    static_fields: &FieldInfo,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    // let name_ref = get_am_ref_name(name);
    let am_group_inner_name = get_inner_am_name(name);
    let mut field_intos_am = quote! {};
    for fname in fields.names() {
        field_intos_am.extend(quote! {
            #fname: am.#fname.clone(),
        });
    }

    let mut static_intos = quote! {};
    for fname in static_fields.names() {
        static_intos.extend(quote! {
            #fname: self.#fname.clone(),
        });
    }

    let (_, ty_generics, _) = generics.split_for_impl();
    quote! {
        fn as_orig_am (& self, am: &#am_group_inner_name #ty_generics) -> #name #ty_generics {
            #name{
                #static_intos
                #field_intos_am
            }
        }
    }
}

fn create_am_group_remote(
    generics: &syn::Generics,
    attributes: &proc_macro2::TokenStream,
    traits: &proc_macro2::TokenStream,
    attrs: &proc_macro2::TokenStream,
    vis: &proc_macro2::TokenStream,
    name: &syn::Ident,
    fields: &FieldInfo,
    static_fields: &FieldInfo,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let am_group_inner_name = get_inner_am_name(name);
    let am_group_name = get_am_group_name(name);
    let am_group_fields = quote! {
        #static_fields
        ams: Vec<#am_group_inner_name #ty_generics>,
    };
    let mut am_group_ser = quote! {
        for e in (&self.ams).iter(){
            e.ser(num_pes,darcs);
        }
    };
    am_group_ser.extend(static_fields.ser());

    let mut am_group_des = quote! {
        for e in (&self.ams).iter(){
            e.des(cur_pe);
        }
    };
    am_group_des.extend(static_fields.des());

    let (the_struct, the_traits) = create_am_struct(
        generics,
        attributes,
        traits,
        attrs,
        vis,
        &am_group_name,
        &am_group_fields,
        &am_group_ser,
        &am_group_des,
        lamellar,
        local,
    );
    let orig_am_ref = create_as_orig_am_ref(generics, name, fields, static_fields, lamellar);
    let orig_am = create_as_orig_am(generics, name, fields, static_fields, lamellar);
    (
        the_struct,
        quote! {
            impl #impl_generics #am_group_name #ty_generics #where_clause{
                #orig_am_ref
                #orig_am
            }

            #the_traits
        },
    )
}

fn create_am_group_local(
    generics: &syn::Generics,
    attributes: &proc_macro2::TokenStream,
    traits: &proc_macro2::TokenStream,
    attrs: &proc_macro2::TokenStream,
    vis: &proc_macro2::TokenStream,
    name: &syn::Ident,
    fields: &FieldInfo,
    static_fields: &FieldInfo,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let am_group_inner_name = get_inner_am_name(name);
    let am_group_name = get_am_group_name(name);
    let am_group_local_name = get_am_group_local_name(name);
    let am_group_local_ser_name = get_am_group_local_ser_name(name);
    let am_group_local_ser_name_str = am_group_local_ser_name.to_string();
    let name_ref = get_am_ref_name(name);

    let am_group_local_fields = quote! {
        #static_fields
        ams: std::sync::Arc<Vec<#am_group_inner_name #ty_generics>>,
        si: usize,
        ei: usize,
    };

    // let local_attrs = if lamellar.to_string() != "crate"  {
    //     // println!("process fields serde_temp_2");
    //     quote! {
    //         #[serde(crate = "lamellar::serde")]
    //         #[serde(into = #am_group_local_ser_name_str)]
    //     }
    // } else {
    //     quote! {#[serde(into = #am_group_local_ser_name_str)]}
    // };

    let mut am_group_local_ser = quote! {
        for e in (&self.ams[self.si..self.ei]).iter(){
            e.ser(num_pes,darcs);
        }
    };
    am_group_local_ser.extend(static_fields.ser());

    let mut am_group_local_des = quote! {
        for e in (&self.ams[self.si..self.ei]).iter(){
            e.des(cur_pe);
        }
    };
    am_group_local_des.extend(static_fields.des());

    let inner_traits = quote! {#[derive(Clone)]};

    let am_group_local_serialize = impl_am_group_local_serialize(
        generics,
        &name,
        &am_group_name,
        &am_group_local_name,
        static_fields,
        lamellar,
    );

    let mut field_intos_self = quote! {};
    for fname in fields.names() {
        field_intos_self.extend(quote! {
            #fname: self.#fname,//.clone(),
        });
    }

    let mut static_intos = quote! {};
    let mut static_ref_intos = quote! {};
    for fname in static_fields.names() {
        static_intos.extend(quote! {
            #fname: self.#fname.clone(),
        });
    }


    let (am_group_local, am_group_local_traits) = create_am_struct(
        generics,
        &attributes,
        &inner_traits,
        &quote! {},
        vis,
        &am_group_local_name,
        &am_group_local_fields,
        &am_group_local_ser,
        &am_group_local_des,
        lamellar,
        local,
    );

    let orig_am_ref = create_as_orig_am_ref(generics, name, fields, static_fields, lamellar);
    let orig_am = create_as_orig_am(generics, name, fields, static_fields, lamellar);
    (
        am_group_local,
        quote! {
            impl #impl_generics #name #ty_generics #where_clause{
                fn as_am_group_local(&self) -> #am_group_local_name {
                    #am_group_local_name{
                        #static_intos
                        ams: std::sync::Arc::new(Vec::new()),
                        si: 0,
                        ei: 0
                    }
                }
                fn into_am_group_inner(self) -> #am_group_inner_name {
                    #am_group_inner_name{
                        #field_intos_self
                    }
                }
            }
            impl #impl_generics #am_group_local_name #ty_generics #where_clause{
                #orig_am_ref
                #orig_am
            }
            #am_group_local_serialize
            #am_group_local_traits
        },
    )
}

fn create_am_group_local_ser(
    generics: &syn::Generics,
    attributes: &proc_macro2::TokenStream,
    traits: &proc_macro2::TokenStream,
    attrs: &proc_macro2::TokenStream,
    vis: &proc_macro2::TokenStream,
    name: &syn::Ident,
    fields: &FieldInfo,
    static_fields: &FieldInfo,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let (_impl_generics, ty_generics, _where_clause) = generics.split_for_impl();
    let am_group_inner_name = get_inner_am_name(name);
    let am_group_name = get_am_group_name(name);
    let am_group_local_name = get_am_group_local_name(name);
    let am_group_local_ser_name = get_am_group_local_ser_name(name);

    let serde_as = format!("Vec<{}>", quote! {#am_group_inner_name #ty_generics});

    let am_group_local_ser_fields = quote! {
        #static_fields
        #[serde_as(as = #serde_as)]
        ams: &'am_local [#name #ty_generics],
    };

    let local_attrs = if lamellar.to_string() != "crate" {
        quote! {
            #[lamellar::serde_with::serde_as(crate = "lamellar::serde_with")]
            #attrs
        }
    } else {
        quote! {
            #attrs
        }
    };

    let mut am_group_local_ser = quote! {
        for e in self.ams{
            e.ser(num_pes,darcs);
        }
    };
    am_group_local_ser.extend(static_fields.ser());

    let mut am_group_local_des = quote! {
        for e in self.ams{
            e.des(cur_pe);
        }
    };
    am_group_local_des.extend(static_fields.des());

    let local_traits = quote! {#[derive(Clone,lamellar::Serialize)]};

    let mut local_ser_generics = generics.clone();
    local_ser_generics
        .params
        .push(syn::GenericParam::Lifetime(syn::LifetimeParam {
            attrs: vec![],
            lifetime: syn::Lifetime::new("'am_local", proc_macro2::Span::call_site()),
            colon_token: None,
            bounds: syn::punctuated::Punctuated::new(),
        }));

    let am_group_local_ser_name_str = am_group_local_ser_name.to_string();
    let (am_group_local_ser, am_group_local_ser_traits) = create_am_struct(
        &local_ser_generics,
        &attributes,
        &local_traits,
        &local_attrs,
        vis,
        &am_group_local_ser_name,
        &am_group_local_ser_fields,
        &am_group_local_ser,
        &am_group_local_des,
        lamellar,
        local,
    );

    let (ser_impl_generics, ser_ty_generics, ser_where_clause) =
        local_ser_generics.split_for_impl();

    let mut static_intos = quote! {};
    for f in static_fields.names() {
        static_intos.extend(quote! {
            #f: am.#f.clone(),
        })
    }

    (
        am_group_local_ser,
        quote! {
            #am_group_local_ser_traits
            impl #ser_impl_generics From<#am_group_local_name #ty_generics> for #am_group_local_ser_name #ser_ty_generics #ser_where_clause{
                fn from(am: #am_group_local_name #ty_generics) -> Self {
                    #am_group_local_ser_name{
                        #static_intos
                        ams: &am.ams[am.si..am.ei],
                    }
                }
            }
        },
    )
}

pub(crate) fn create_am_group_structs(
    generics: &syn::Generics,
    attributes: &proc_macro2::TokenStream,
    traits: &proc_macro2::TokenStream,
    attrs: &proc_macro2::TokenStream,
    vis: &proc_macro2::TokenStream,
    name: &syn::Ident,
    fields: &FieldInfo,
    static_fields: &FieldInfo,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let am_ref = create_am_ref(
        generics,
        attributes,
        traits,
        attrs,
        vis,
        name,
        fields,
        static_fields,
        lamellar,
        local,
    );

    let (am_group_inner, am_group_inner_traits) = create_am_group_inner(
        generics,
        attributes,
        traits,
        attrs,
        vis,
        name,
        fields,
        static_fields,
        lamellar,
        local,
    );

    let (am_group_remote, am_group_remote_traits) = create_am_group_remote(
        generics,
        attributes,
        traits,
        attrs,
        vis,
        name,
        fields,
        static_fields,
        lamellar,
        local,
    );

    let (am_group_local, am_group_local_traits) = create_am_group_local(
        generics,
        attributes,
        traits,
        attrs,
        vis,
        name,
        fields,
        static_fields,
        lamellar,
        local,
    );

    // let (am_group_local_ser, am_group_local_ser_traits) = create_am_group_local_ser(generics, attributes, traits, attrs, vis, name, fields, static_fields, lamellar, local);

    // println!("{:?}", am_group_inner.to_string());
    // println!("{:?}", am_group_remote.to_string());
    // println!("{:?}", am_group_local.to_string());

    (
        quote! {
            #am_ref
            #am_group_inner
            #am_group_remote
            #am_group_local
            // #am_group_local_ser
        },
        quote! {

            #am_group_inner_traits
            #am_group_remote_traits
            #am_group_local_traits
            // #am_group_local_ser_traits
        },
    )
}
