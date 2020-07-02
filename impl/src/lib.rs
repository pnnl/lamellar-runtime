extern crate proc_macro;


use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use regex::Regex;
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream, Result};
use syn::parse_macro_input;
use syn::visit_mut::VisitMut;

#[derive(Debug)]
struct FormatArgs {
    format_string: syn::Expr,
    positional_args: Vec<syn::Expr>,
    named_args: Vec<(syn::Ident, syn::Expr)>,
}

impl Parse for FormatArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let format_string: syn::Expr;
        let mut positional_args = Vec::new();
        let mut named_args = Vec::new();

        format_string = input.parse()?;
        while !input.is_empty() {
            input.parse::<syn::Token![,]>()?;
            if input.is_empty() {
                break;
            }
            if input.peek(syn::Ident::peek_any) && input.peek2(syn::Token![=]) {
                while !input.is_empty() {
                    let name: syn::Ident = input.call(syn::Ident::parse_any)?;
                    input.parse::<syn::Token![=]>()?;
                    let value: syn::Expr = input.parse()?;
                    named_args.push((name, value));
                    if input.is_empty() {
                        break;
                    }
                    input.parse::<syn::Token![,]>()?;
                }
                break;
            }
            positional_args.push(input.parse()?);
        }

        Ok(FormatArgs {
            format_string,
            positional_args,
            named_args,
        })
    }
}

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
            panic!("unrecognized macro {:?} in lamellar::am expansion can currently only handle format like macros", i);
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
            for i in 1..tok_str.len() {
                if cur_pe_re.is_match(&tok_str[i].to_string()) {
                    new_tok_str += &(",".to_owned() + "__lamellar_current_pe");
                } else if num_pes_re.is_match(&tok_str[i].to_string()) {
                    new_tok_str += &(",".to_owned() + "__lamellar_num_pes");
                } else {
                    new_tok_str += &(",".to_owned() + &tok_str[i].to_string());
                }
            }
            // println!("new_tok_str {:?}", new_tok_str);

            i.tokens = new_tok_str.parse().unwrap();
        } else {
            panic!("unrecognized macro {:?} in lamellar::am expansion can currently only handle format like macros", i);
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
    // println!("stmt: {:#?}",stmt);
    LamellarDSLReplace.visit_stmt_mut(&mut stmt);
    // println!("after stmt: {:#?}",stmt);
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
            fn exec(&self,__lamellar_current_pe: isize,__lamellar_num_pes: isize, local: bool) -> Option<lamellar::LamellarReturn>{
                #temp
                None
            }
            fn get_id(&self) -> String{
                stringify!(#orig_name).to_string()
            }
            fn ser(&self) -> Vec<u8> {
                lamellar::bincode::serialize(self).expect("error in serialization")
            }
        }

        impl LamellarAM for #orig_name {
            type Output = ();
            // fn exec(&self,__lamellar_current_pe: isize,__lamellar_num_pes: isize) -> Self::Output {
            fn exec(&self,) -> Self::Output {
                let __lamellar_current_pe = -1;
                let __lamellar_num_pes = -1;
                #temp
            }
        }

        fn #orig_name_exec(bytes: Vec<u8>,__lamellar_current_pe: isize,__lamellar_num_pes: isize) -> Option<lamellar::LamellarReturn> {
            let __lamellar_data: #orig_name = lamellar::bincode::deserialize(&bytes).expect("error in deserializing to");// as lamellar::LamellarActiveMessage;
            <#orig_name as lamellar::LamellarActiveMessage>::exec(&__lamellar_data,__lamellar_current_pe,__lamellar_num_pes,false)
            // __lamellar_data.exec(__lamellar_current_pe,__lamellar_num_pes)
            // #new_temp
            // None
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

fn am_with_return(input: syn::ItemImpl, output: syn::Type) -> TokenStream {
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
            fn exec(&self,__lamellar_current_pe: isize,__lamellar_num_pes: isize, local: bool) -> Option<lamellar::LamellarReturn>{
                #temp
                let ret = lamellar::bincode::serialize(& #last_expr ).expect("error serializing return");
                let ret = match local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                    true => Some(lamellar::LamellarReturn::LocalData(ret)),
                    false => Some(lamellar::LamellarReturn::RemoteData(ret)),
                };
                ret
            }
            fn get_id(&self) -> String{
                stringify!(#orig_name).to_string()
            }
            fn ser(&self) -> Vec<u8> {
                lamellar::bincode::serialize(self).expect("error in serialization")
            }
        }

        impl LamellarAM for #orig_name {
            type Output = #output;
            // fn exec(&self,__lamellar_current_pe: isize,__lamellar_num_pes: isize) -> Self::Output {
            fn exec(&self,) -> Self::Output {
                let __lamellar_current_pe = -1;
                    let __lamellar_num_pes = -1;
                #temp
                #last_expr
            }
        }

        fn #orig_name_exec(bytes: Vec<u8>,__lamellar_current_pe: isize,__lamellar_num_pes: isize) -> Option<lamellar::LamellarReturn> {
            let __lamellar_data: #orig_name = lamellar::bincode::deserialize(&bytes).expect("error in deserializing to");
            <#orig_name as lamellar::LamellarActiveMessage>::exec(&__lamellar_data,__lamellar_current_pe,__lamellar_num_pes,false)
            // #new_temp
            // let ret = bincode::serialize(& #last_expr_no_self ).expect("error serializing return");
            // let ret = Some(lamellar::LamellarReturn::RemoteData(ret));
            // ret
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
    if return_am.find("(") != None {
        let temp = return_am.split("(").collect::<Vec<&str>>();
        return_type = temp
            .last()
            .expect("error in lamellar::am argument")
            .trim_matches(&[')'][..])
            .to_string();
        // return_am = temp
        //     .first()
        //     .expect("error in lamellar::am argument")
        //     .to_string();
    }
    // println!("return_am {:?} return_type {:?}", return_am, return_type);
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
                // fn exec(&self,__lamellar_current_pe: isize,__lamellar_num_pes: isize) -> Self::Output {
                fn exec(&self) -> Self::Output {
                    let __lamellar_current_pe = -1;
                    let __lamellar_num_pes = -1;
                    #temp
                    // #last_expr.exec(__lamellar_current_pe,__lamellar_num_pes)
                    #last_expr.exec()
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
                // fn exec(&self,__lamellar_current_pe: isize,__lamellar_num_pes: isize) -> Self::Output {
                fn exec(&self) -> Self::Output {
                    let __lamellar_current_pe = -1;
                    let __lamellar_num_pes = -1;
                    #temp
                    // #last_expr.exec(__lamellar_current_pe,__lamellar_num_pes)
                    #last_expr.exec()
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
            fn exec(&self,__lamellar_current_pe: isize,__lamellar_num_pes: isize, local: bool) -> Option<lamellar::LamellarReturn>{
                #temp
                let ret = Box::new (
                     #last_expr
                );
                let ret = match local{
                    true => Some(lamellar::LamellarReturn::LocalAm(ret)),
                    false => Some(lamellar::LamellarReturn::RemoteAm(ret)),
                };
                ret
            }
            fn get_id(&self) -> String{
                stringify!(#orig_name).to_string()
            }
            fn ser(&self) -> Vec<u8> {
                lamellar::bincode::serialize(self).expect("error in serialization")
            }
        }

        fn #orig_name_exec(bytes: Vec<u8>,__lamellar_current_pe: isize,__lamellar_num_pes: isize) -> Option<lamellar::LamellarReturn> {
            let __lamellar_data: #orig_name = lamellar::bincode::deserialize(&bytes).expect("error in deserializing to");
            <#orig_name as lamellar::LamellarActiveMessage>::exec(&__lamellar_data,__lamellar_current_pe,__lamellar_num_pes,false)
            // #new_temp
            // let am = Box::new (
            //          #last_expr_no_self
            //     );
            // let ret = Some(lamellar::LamellarReturn::RemoteAm(am));
            // ret
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
            let output = get_return_of_method("exec".to_string(), &input.items);
            match output {
                Some(output) => {
                    if args.len() > 0 {
                        am_with_return_am(input, output, args)
                    } else {
                        am_with_return(input, output)
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
