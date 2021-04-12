extern crate proc_macro;

mod parse;

use crate::parse::{FormatArgs, ReductionArgs};

use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_error::proc_macro_error;
use quote::quote;
use regex::Regex;
use syn::parse::Result;
use syn::parse_macro_input;
use syn::visit_mut::VisitMut;

struct SelfReplace;

struct LamellarDSLReplace;

impl VisitMut for SelfReplace {
    fn visit_ident_mut(&mut self, i: &mut syn::Ident) {
        // println!("ident: {:?}",i);
        if i.to_string() == "self" {
            *i = syn::Ident::new("__lamellar_data", Span::call_site());
        }
        // println!("ident: {:?}",i);
        syn::visit_mut::visit_ident_mut(self, i);
    }

    fn visit_macro_mut(&mut self, i: &mut syn::Macro) {
        let args: Result<FormatArgs> = i.parse_body();

        if args.is_ok() {
            let tok_str = i.tokens.to_string();
            let tok_str = tok_str.split(",").collect::<Vec<&str>>();
            let mut new_tok_str: String = tok_str[0].to_string();
            for i in 1..tok_str.len() {
                new_tok_str +=
                    &(",".to_owned() + &tok_str[i].to_string().replace("self", "__lamellar_data"));
            }
            i.tokens = new_tok_str.parse().unwrap();
        } else {
            // println!("warning unrecognized macro {:?} in lamellar::am expansion can currently only handle format like macros", i);
        }
        syn::visit_mut::visit_macro_mut(self, i);
    }
}

impl VisitMut for LamellarDSLReplace {
    fn visit_ident_mut(&mut self, i: &mut syn::Ident) {
        match i.to_string().as_str() {
            "lamellar::current_pe" => {
                *i = syn::Ident::new("__lamellar_current_pe", Span::call_site());
            }
            "lamellar::num_pes" => {
                *i = syn::Ident::new("__lamellar_num_pes", Span::call_site());
            }
            "lamellar::world" => {
                *i = syn::Ident::new("__lamellar_world", Span::call_site());
            }
            "lamellar::team" => {
                *i = syn::Ident::new("__lamellar_team", Span::call_site());
            }
            _ => {}
        }
        syn::visit_mut::visit_ident_mut(self, i);
    }
    fn visit_path_mut(&mut self, i: &mut syn::Path) {
        // println!("seg len: {:?}", i.segments.len());
        if i.segments.len() == 2 {
            if let Some(pathseg) = i.segments.first() {
                if pathseg.ident.to_string() == "lamellar" {
                    if let Some(pathseg) = i.segments.last() {
                        match pathseg.ident.to_string().as_str() {
                            "current_pe" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new(
                                        "__lamellar_current_pe",
                                        Span::call_site(),
                                    ),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            "num_pes" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new("__lamellar_num_pes", Span::call_site()),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            "world" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new("__lamellar_world", Span::call_site()),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            "team" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new("__lamellar_team", Span::call_site()),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        syn::visit_mut::visit_path_mut(self, i);
    }

    fn visit_macro_mut(&mut self, i: &mut syn::Macro) {
        let args: Result<FormatArgs> = i.parse_body();

        if args.is_ok() {
            let tok_str = i.tokens.to_string();
            let tok_str = tok_str.split(",").collect::<Vec<&str>>();
            let mut new_tok_str: String = tok_str[0].to_string();
            let cur_pe_re = Regex::new("lamellar(?s:.)*::(?s:.)*current_pe").unwrap();
            let num_pes_re = Regex::new("lamellar(?s:.)*::(?s:.)*num_pes").unwrap();
            let world_re = Regex::new("lamellar(?s:.)*::(?s:.)*world").unwrap();
            let team_re = Regex::new("lamellar(?s:.)*::(?s:.)*team").unwrap();
            for i in 1..tok_str.len() {
                if cur_pe_re.is_match(&tok_str[i].to_string()) {
                    new_tok_str += &(",".to_owned() + "__lamellar_current_pe");
                } else if num_pes_re.is_match(&tok_str[i].to_string()) {
                    new_tok_str += &(",".to_owned() + "__lamellar_num_pes");
                } else if world_re.is_match(&tok_str[i].to_string()) {
                    new_tok_str += &(",".to_owned() + "__lamellar_world");
                } else if team_re.is_match(&tok_str[i].to_string()) {
                    new_tok_str += &(",".to_owned() + "__lamellar_team");
                } else {
                    new_tok_str += &(",".to_owned() + &tok_str[i].to_string());
                }
            }
            // println!("new_tok_str {:?}", new_tok_str);

            i.tokens = new_tok_str.parse().unwrap();
        } else {
            // println!("warning unrecognized macro {:?} in lamellar::am expansion can currently only handle format like macros", i);
        }
        syn::visit_mut::visit_macro_mut(self, i);
    }
}

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

fn replace_self(mut stmt: syn::Stmt) -> syn::Stmt {
    SelfReplace.visit_stmt_mut(&mut stmt);
    stmt
}

fn replace_lamellar_dsl(mut stmt: syn::Stmt) -> syn::Stmt {
    LamellarDSLReplace.visit_stmt_mut(&mut stmt);
    stmt
}

fn am_without_return(input: syn::ItemImpl) -> TokenStream {
    // println!("{:#?}", input);
    let name = type_name(&input.self_ty).expect("unable to find name");
    let exec_fn =
        get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
    let stmts = exec_fn.stmts;
    let mut temp = quote! {};
    let mut new_temp = quote! {};

    for stmt in stmts {
        // println!("{:?}", stmt);
        let new_stmt = replace_lamellar_dsl(stmt.clone());
        let selfless_stmt = replace_self(new_stmt.clone()); //replaces self with "__lamellar_data" (a named instance of the underlying struct type of self)
        new_temp.extend(quote! {
            #selfless_stmt
        });

        temp.extend(quote! {
            #new_stmt
        });
    }
    // new_stmttln!("name: {:?}", name + "Result");
    let orig_name = syn::Ident::new(&name, Span::call_site());
    let orig_name_exec = quote::format_ident!("{}_exec", orig_name.clone());

    let expanded = quote! {
        impl lamellar::LamellarActiveMessage for #orig_name {
            fn exec(self: Box<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, local: bool, __lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<lamellar::LamellarReturn>> + Send>>{

                Box::pin( async move {
                #temp
                None
                })

            }
            fn get_id(&self) -> String{
                stringify!(#orig_name).to_string()
            }
            fn ser(&self) -> Vec<u8> {
                lamellar::serialize(self).unwrap()
            }
        }

        impl LamellarAM for #orig_name {
            type Output = ();
            fn exec(self,__lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> Self::Output {
                let __lamellar_current_pe = 0;
                let __lamellar_num_pes = 0;

                lamellar::async_std::task::block_on(async move {
                    #temp
                })

            }
        }

        fn #orig_name_exec(bytes: Vec<u8>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<lamellar::LamellarReturn>> + Send>> {
            let __lamellar_data: Box<#orig_name> = Box::new(lamellar::deserialize(&bytes).unwrap());
            <#orig_name as lamellar::LamellarActiveMessage>::exec(__lamellar_data,__lamellar_current_pe,__lamellar_num_pes,false,__lamellar_world,__lamellar_team)
        }

        lamellar::inventory::submit! {
            #![crate = lamellar]
            lamellar::RegisteredAm{
                exec: #orig_name_exec,
                name: stringify!(#orig_name).to_string()
            }
        }
    };
    TokenStream::from(expanded)
}

fn am_with_return(input: syn::ItemImpl, output: syn::Type, crate_header: String) -> TokenStream {
    let name = type_name(&input.self_ty).expect("unable to find name");
    let mut exec_fn =
        get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
    let last_stmt = replace_lamellar_dsl(exec_fn.stmts.pop().unwrap().clone());
    let last_expr = get_expr(&last_stmt)
        .expect("failed to get exec return value (try removing the last \";\")");
    let _last_expr_no_self = get_expr(&replace_self(last_stmt.clone()))
        .expect("failed to get exec return value (try removing the last \";\")");

    let stmts = exec_fn.stmts;
    let mut temp = quote! {};
    let mut new_temp = quote! {};

    for stmt in stmts {
        let new_stmt = replace_lamellar_dsl(stmt.clone());
        let selfless_stmt = replace_self(new_stmt.clone()); //replaces self with "__lamellar_data" (a named instance of the underlying struct type of self)
        new_temp.extend(quote! {
            #selfless_stmt
        });

        temp.extend(quote! {
            #new_stmt
        });
    }

    let orig_name = syn::Ident::new(&name, Span::call_site());

    let orig_name_exec = quote::format_ident!("{}_exec", orig_name.clone());

    let lamellar = quote::format_ident!("{}", crate_header.clone());

    let expanded = quote! {
        impl #lamellar::LamellarActiveMessage for #orig_name {
            fn exec(self: Box<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<#lamellar::LamellarReturn>> + Send>>{

                Box::pin( async move {
                #temp
                let ret = #lamellar::serialize(& #last_expr ).unwrap();
                let ret = match local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                    true => Some(#lamellar::LamellarReturn::LocalData(ret)),
                    false => Some(#lamellar::LamellarReturn::RemoteData(ret)),
                };
                ret
                })
            }
            fn get_id(&self) -> String{
                stringify!(#orig_name).to_string()
            }
            fn ser(&self) -> Vec<u8> {
                #lamellar::serialize(self).unwrap()
            }
        }

        impl LamellarAM for #orig_name {
            type Output = #output;
            fn exec(self,__lamellar_world: std::sync::Arc<#lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeamRT>) -> Self::Output {
                panic!("not valid to execute an active message on a non worker thread");
                // let __lamellar_current_pe = 0;
                // let __lamellar_num_pes = 0;
                // #lamellar::async_std::task::block_on(async move {
                //     #temp
                //     #last_expr
                // })
            }
        }

        fn #orig_name_exec(bytes: Vec<u8>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<#lamellar::LamellarReturn>> + Send>> {
            let __lamellar_data: Box<#orig_name> = Box::new(#lamellar::deserialize(&bytes).unwrap());
            <#orig_name as #lamellar::LamellarActiveMessage>::exec(__lamellar_data,__lamellar_current_pe,__lamellar_num_pes,false,__lamellar_world,__lamellar_team)
        }

        #lamellar::inventory::submit! {
            #![crate = #lamellar]
            #lamellar::RegisteredAm{
                exec: #orig_name_exec,
                name: stringify!(#orig_name).to_string()
            }
        }
    };
    TokenStream::from(expanded)
}

fn am_with_return_am(input: syn::ItemImpl, _output: syn::Type, args: String) -> TokenStream {
    // println!("{:#?}", input);
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
    let name = type_name(&input.self_ty).expect("unable to find name");
    let mut exec_fn =
        get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
    let last_stmt = replace_lamellar_dsl(exec_fn.stmts.last().unwrap().clone());
    let last_expr = get_expr(&last_stmt)
        .expect("failed to get exec return value (try removing the last \";\")");
    let _last_expr_no_self = get_expr(&replace_self(last_stmt.clone()))
        .expect("failed to get exec return value (try removing the last \";\")");

    exec_fn.stmts.pop();
    let stmts = exec_fn.stmts;
    let mut temp = quote! {};
    let mut new_temp = quote! {};

    for stmt in stmts {
        let new_stmt = replace_lamellar_dsl(stmt.clone());
        let selfless_stmt = replace_self(new_stmt.clone()); //replaces self with "__lamellar_data" (a named instance of the underlying struct type of self)
        new_temp.extend(quote! {
            #selfless_stmt
        });

        temp.extend(quote! {
            #new_stmt
        });
    }

    let orig_name = syn::Ident::new(&name, Span::call_site());
    let return_name = syn::Ident::new(&(name + "Result"), Span::call_site());
    let orig_name_exec = quote::format_ident!("{}_exec", orig_name.clone());

    let ret_temp = if return_type.len() > 0 {
        let return_type = syn::Ident::new(&return_type, Span::call_site());
        quote! {
            impl LamellarAM for #orig_name {
                type Output = #return_type;
                fn exec(self, __lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> Self::Output {
                    panic!("not valid to execute an active message on a non worker thread");
                    // let __lamellar_current_pe = -1;
                    // let __lamellar_num_pes = -1;
                    // lamellar::async_std::task::block_on(async move {
                    // #temp
                    // #last_expr.exec(__lamellar_team,__lamellar_world)
                    // })
                }
            }
            #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
            struct #return_name {
                output: #return_type
            }
        }
    } else {
        quote! {
            impl LamellarAM for #orig_name {
                type Output = ();
                fn exec(self, __lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> Self::Output {
                    panic!("not valid to execute an active message on a non worker thread");
                    // let __lamellar_current_pe = -1;
                    // let __lamellar_num_pes = -1;
                    // lamellar::async_std::task::block_on(async move {
                    // #temp
                    // #last_expr.exec(__lamellar_team,__lamellar_world)
                    // })
                }
            }
            #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
            struct #return_name {
                output: ()
            }
        }
    };
    let expanded = quote! {
        #ret_temp

        impl lamellar::LamellarActiveMessage for #orig_name {
            fn exec(self: Box<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, local: bool, __lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<lamellar::LamellarReturn>> + Send>> {
                Box::pin(async move {
                #temp
                let ret = Box::new (
                     #last_expr
                );
                let ret = match local{
                    true => Some(lamellar::LamellarReturn::LocalAm(ret )),
                    false => Some(lamellar::LamellarReturn::RemoteAm(ret )),
                };
                ret
                })
            }
            fn get_id(&self) -> String{
                stringify!(#orig_name).to_string()
            }
            fn ser(&self) -> Vec<u8> {
                lamellar::serialize(self).unwrap()
            }
        }

        fn #orig_name_exec(bytes: Vec<u8>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<lamellar::LamellarReturn>> + Send>>  {
            let __lamellar_data: Box<#orig_name> = Box::new(lamellar::deserialize(&bytes).unwrap());
            <#orig_name as lamellar::LamellarActiveMessage>::exec(__lamellar_data,__lamellar_current_pe,__lamellar_num_pes,false,__lamellar_world,__lamellar_team)
        }

        lamellar::inventory::submit! {
            #![crate = lamellar]
            lamellar::RegisteredAm{
                exec: #orig_name_exec,
                name: stringify!(#orig_name).to_string()
            }
        }
    };
    TokenStream::from(expanded)
}

#[proc_macro_error]
#[proc_macro_attribute]
pub fn am(args: TokenStream, input: TokenStream) -> TokenStream {
    // println!("in am expansion!");
    let args = args.to_string();
    if args.len() > 0 {
        assert!(args.starts_with("return_am"), "#[lamellar::am] only accepts an (optional) argument of the form:
                                               #[lamellar::am(return_am = \"<am to exec upon return>\")]");
    }
    // println!("args: {:?}", args);
    let input: syn::Item = parse_macro_input!(input);

    let output = match input.clone() {
        syn::Item::Impl(input) => {
            // println!("{:?}",input);
            let output = get_return_of_method("exec".to_string(), &input.items);
            match output {
                Some(output) => {
                    if args.len() > 0 {
                        am_with_return_am(input, output, args)
                    } else {
                        am_with_return(input, output, "lamellar".to_string())
                    }
                }

                None => {
                    if args.len() > 0 {
                        panic!("no return type detected (try removing the last \";\"), but parameters passed into [#lamellar::am]
                        #[lamellar::am] only accepts an (optional) argument of the form:
                        #[lamellar::am(return_am = \"<am to exec upon return>\")]");
                    }
                    am_without_return(input)
                }
            }
        }
        _ => {
            println!("lamellar am attribute only valid for impl blocks");
            let output = quote! { #input };
            output.into()
        }
    };
    // println!("leaving expansion");
    output
}

#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am(args: TokenStream, input: TokenStream) -> TokenStream {
    // println!("in am expansion!");
    let args = args.to_string();
    if args.len() > 0 {
        assert!(args.starts_with("return_am"), "#[lamellar::am] only accepts an (optional) argument of the form:
                                               #[lamellar::am(return_am = \"<am to exec upon return>\")]");
    }
    // println!("args: {:?}", args);
    let input: syn::Item = parse_macro_input!(input);

    let output = match input.clone() {
        syn::Item::Impl(input) => {
            // println!("{:?}",input);
            let output = get_return_of_method("exec".to_string(), &input.items);
            match output {
                Some(output) => {
                    if args.len() > 0 {
                        am_with_return_am(input, output, args)
                    } else {
                        am_with_return(input, output, "crate".to_string())
                    }
                }

                None => {
                    if args.len() > 0 {
                        panic!("no return type detected (try removing the last \";\"), but parameters passed into [#lamellar::am]
                        #[lamellar::am] only accepts an (optional) argument of the form:
                        #[lamellar::am(return_am = \"<am to exec upon return>\")]");
                    }
                    am_without_return(input)
                }
            }
        }
        _ => {
            println!("lamellar am attribute only valid for impl blocks");
            let output = quote! { #input };
            output.into()
        }
    };
    // println!("leaving expansion");
    output
}

fn reduction_with_return(input: syn::ItemImpl, output: syn::Type) -> TokenStream {
    let name = type_name(&input.self_ty).expect("unable to find name");
    let mut exec_fn =
        get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
    let last_stmt = replace_lamellar_dsl(exec_fn.stmts.pop().unwrap().clone());
    let last_expr = get_expr(&last_stmt)
        .expect("failed to get exec return value (try removing the last \";\")");
    let _last_expr_no_self = get_expr(&replace_self(last_stmt.clone()))
        .expect("failed to get exec return value (try removing the last \";\")");

    let stmts = exec_fn.stmts;
    let mut temp = quote! {};
    let mut new_temp = quote! {};

    for stmt in stmts {
        let new_stmt = replace_lamellar_dsl(stmt.clone());
        let selfless_stmt = replace_self(new_stmt.clone()); //replaces self with "__lamellar_data" (a named instance of the underlying struct type of self)
        new_temp.extend(quote! {
            #selfless_stmt
        });

        temp.extend(quote! {
            #new_stmt
        });
    }

    let orig_name = syn::Ident::new(&name, Span::call_site());

    let orig_name_exec = quote::format_ident!("{}_exec", orig_name.clone());

    let expanded = quote! {
        impl lamellar::LamellarActiveMessage for #orig_name {
            fn exec(self: Box<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, local: bool, __lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<lamellar::LamellarReturn>> + Send>>{

                Box::pin( async move {
                #temp
                let ret = lamellar::serialize(& #last_expr ).unwrap();
                let ret = match local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                    true => Some(lamellar::LamellarReturn::LocalData(ret)),
                    false => Some(lamellar::LamellarReturn::RemoteData(ret)),
                };
                ret
                })
            }
            fn get_id(&self) -> String{
                stringify!(#orig_name).to_string()
            }
            fn ser(&self) -> Vec<u8> {
                lamellar::serialize(self).unwrap()
            }
        }

        impl LamellarAM for #orig_name {
            type Output = #output;
            fn exec(self,__lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> Self::Output {
                let __lamellar_current_pe = -1;
                let __lamellar_num_pes = -1;
                lamellar::async_std::task::block_on(async move {
                    #temp
                    #last_expr
                })
            }
        }

        fn #orig_name_exec(bytes: Vec<u8>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<lamellar::LamellarReturn>> + Send>> {
            let __lamellar_data: Box<#orig_name> = Box::new(lamellar::deserialize(&bytes).unwrap());
            <#orig_name as lamellar::LamellarActiveMessage>::exec(__lamellar_data,__lamellar_current_pe,__lamellar_num_pes,false,__lamellar_world,__lamellar_team)
        }

        lamellar::inventory::submit! {
            #![crate = lamellar]
            lamellar::RegisteredAm{
                exec: #orig_name_exec,
                name: stringify!(#orig_name).to_string()
            }
        }
    };
    TokenStream::from(expanded)
}

#[proc_macro_error]
#[proc_macro_attribute]
pub fn reduction(args: TokenStream, input: TokenStream) -> TokenStream {
    // println!("in am expansion!");
    let args = args.to_string();
    if args.len() > 0 {
        panic!("#[lamellar::reduction] does not expect any arguments");
    }
    // println!("args: {:?}", args);
    let input: syn::Item = parse_macro_input!(input);

    let output = match input.clone() {
        syn::Item::Impl(input) => {
            // println!("{:?}",input);
            if let Some(output) = get_return_of_method("exec".to_string(), &input.items) {
                reduction_with_return(input, output)
            } else {
                let output = quote! { #input };
                output.into()
            }
        }
        _ => {
            println!("lamellar am attribute only valid for impl blocks");
            let output = quote! { #input };
            output.into()
        }
    };
    // println!("leaving expansion");
    output
}

fn create_reduction(
    typeident: syn::Ident,
    reduction: String,
    op: proc_macro2::TokenStream,
    crate_header: String,
) -> proc_macro2::TokenStream {
    let reduction_name = quote::format_ident!("{:}_{:}_reduction", typeident, reduction);
    let reduction_exec = quote::format_ident!("{:}_{:}_reduction_exec", typeident, reduction);
    let reduction_gen = quote::format_ident!("{:}_{:}_reduction_gen", typeident, reduction);
    let reduction = quote::format_ident!("{:}", reduction);
    let lamellar = quote::format_ident!("{}", crate_header.clone());
    let exec = quote! {
       
        if self.start_pe == self.end_pe{
            // println!("[{:?}] {:?}",__lamellar_current_pe,self);
            let timer = std::time::Instant::now();
            let data_slice = <#lamellar::LamellarMemoryRegion<#typeident> as #lamellar::RegisteredMemoryRegion>::as_slice(&self.data).unwrap();
            let first = data_slice.first().unwrap().clone();
            let res = data_slice[1..].iter().fold(first, #op );
            // println!("[{:?}] {:?} {:?}",__lamellar_current_pe,res,timer.elapsed().as_secs_f64());
            res
        }
        else{
            // println!("[{:?}] {:?}",__lamellar_current_pe,self);
            let mid_pe = (self.start_pe + self.end_pe)/2;
            let op = #op;
            let timer = std::time::Instant::now();
            let left = __lamellar_team.exec_am_pe(self.start_pe,  #reduction_name { data: self.data.clone(), start_pe: self.start_pe, end_pe: mid_pe}).into_future();
            let right = __lamellar_team.exec_am_pe(mid_pe+1,  #reduction_name { data: self.data.clone(), start_pe: mid_pe+1, end_pe: self.end_pe}).into_future();
            let res = op(left.await.unwrap(),&right.await.unwrap());
            // println!("[{:?}] {:?} {:?}",__lamellar_current_pe,res,timer.elapsed().as_secs_f64());
            res
        }
    };

    

    let expanded = quote! {
        #[allow(non_camel_case_types)]
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
        struct #reduction_name{
            data: #lamellar::LamellarMemoryRegion<#typeident>,
            start_pe: usize,
            end_pe: usize,
        }
        impl #lamellar::LamellarActiveMessage for #reduction_name {
            fn exec(self: Box<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<#lamellar::LamellarReturn>> + Send>>{

                Box::pin( async move {
                let ret = #lamellar::serialize(& #exec ).unwrap();
                let ret = match local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                    true => Some(#lamellar::LamellarReturn::LocalData(ret)),
                    false => Some(#lamellar::LamellarReturn::RemoteData(ret)),
                };
                ret
                })
            }
            fn get_id(&self) -> String{
                stringify!(#reduction_name).to_string()
            }
            fn ser(&self) -> Vec<u8> {
                #lamellar::serialize(self).unwrap()
            }
        }

        impl LamellarAM for #reduction_name {
            type Output = #typeident;
            fn exec(self,__lamellar_world: std::sync::Arc<#lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeamRT>) -> Self::Output {
                let __lamellar_current_pe = -1;
                let __lamellar_num_pes = -1;
                #lamellar::async_std::task::block_on(async move {
                   #exec
                })
            }
        }

        fn #reduction_exec(bytes: Vec<u8>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<#lamellar::LamellarReturn>> + Send>> {
            let __lamellar_data: Box<#reduction_name> = Box::new(#lamellar::deserialize(&bytes).unwrap());
            <#reduction_name as #lamellar::LamellarActiveMessage>::exec(__lamellar_data,__lamellar_current_pe,__lamellar_num_pes,false,__lamellar_world,__lamellar_team)
        }

        fn  #reduction_gen<T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static> (rmr: #lamellar::LamellarMemoryRegion<T>, num_pes: usize)
        -> Box<dyn #lamellar::LamellarActiveMessage + Send + Sync >{
            Box::new(#reduction_name{data: unsafe {rmr.as_base::<#typeident>() }, start_pe: 0, end_pe: num_pes-1})
        }

        #lamellar::inventory::submit! {
            #![crate = #lamellar]
            #lamellar::RegisteredAm{
                exec: #reduction_exec,
                name: stringify!(#reduction_name).to_string()
            }
        }

        #lamellar::inventory::submit! {
            #![crate = #lamellar]
            #lamellar::ReduceKey{
                id: std::any::TypeId::of::<#typeident>(),
                name: stringify!(#reduction).to_string(),
                gen: #reduction_gen
            }
        }
    };
    expanded
}

#[proc_macro_error]
#[proc_macro]
pub fn register_reduction(item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(item as ReductionArgs);
    let mut output = quote! {};

    for ty in args.tys {
        let mut closure = args.closure.clone();
        let tyc = ty.clone();
        if let syn::Pat::Ident(a) = &closure.inputs[0] {
            let pat: syn::PatType = syn::PatType {
                attrs: vec![],
                pat: Box::new(syn::Pat::Ident(a.clone())),
                colon_token: syn::Token![:](Span::call_site()),
                ty: Box::new(syn::Type::Path(tyc.clone())),
            };
            closure.inputs[0] = syn::Pat::Type(pat);
        }
        if let syn::Pat::Ident(b) = &closure.inputs[1] {
            let tyr: syn::TypeReference = syn::parse_quote! {&#tyc};
            let pat: syn::PatType = syn::PatType {
                attrs: vec![],
                pat: Box::new(syn::Pat::Ident(b.clone())),
                colon_token: syn::Token![:](Span::call_site()),
                ty: Box::new(syn::Type::Reference(tyr)),
            };
            closure.inputs[1] = syn::Pat::Type(pat);
        }

        output.extend(create_reduction(
            ty.path.segments[0].ident.clone(),
            args.name.to_string(),
            quote! {#closure},
            "lamellar".to_string(),
        ));
    }
    TokenStream::from(output)
}

#[proc_macro_error]
#[proc_macro]
pub fn generate_reductions_for_type(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    // let lamellar = quote::format_ident!("{}", "lamellar");
    for t in item.to_string().split(",").collect::<Vec<&str>>() {
        let typeident = quote::format_ident!("{:}", t.trim());
        output.extend(create_reduction(
            typeident.clone(),
            "sum".to_string(),
            quote! {
                let data_slice = <lamellar::LamellarMemoryRegion<#typeident> as lamellar::RegisteredMemoryRegion>::as_slice(&self.data).unwrap();
                let first = data_slice.first().unwrap().clone();
                data_slice[1..].iter().fold(first,|acc,val|{ acc+val } )
            },
            "lamellar".to_string(),
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "prod".to_string(),
            quote! {
                let data_slice = <lamellar::LamellarMemoryRegion<#typeident> as lamellar::RegisteredMemoryRegion>::as_slice(&self.data).unwrap();
                let first = data_slice.first().unwrap().clone();
                data_slice[1..].iter().fold(first,|acc,val|{ acc*val } )
            },
            "lamellar".to_string(),
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "max".to_string(),
            quote! {
                *<lamellar::LamellarMemoryRegion<#typeident> as lamellar::RegisteredMemoryRegion>::as_slice(&self.data).unwrap().iter().max().unwrap()
            },
            "lamellar".to_string(),
        ));
    }

    TokenStream::from(output)
}
#[proc_macro_error]
#[proc_macro]
pub fn generate_reductions_for_type_rt(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    for t in item.to_string().split(",").collect::<Vec<&str>>() {
        let t = t.trim().to_string();
        let typeident = quote::format_ident!("{:}", t.clone());
        output.extend(create_reduction(
            typeident.clone(),
            "sum".to_string(),
            quote! {
                |acc: #typeident, val: &#typeident|{ acc+*val }
            },
            "crate".to_string(),
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "prod".to_string(),
            quote! {
                |acc: #typeident, val: &#typeident| { acc* *val }
            },
            "crate".to_string(),
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "max".to_string(),
            quote! {
                |val1: #typeident, val2: &#typeident| { if val1 > *val2 {val1} else {*val2} }
            },
            "crate".to_string(),
        ));
    }

    TokenStream::from(output)
}
