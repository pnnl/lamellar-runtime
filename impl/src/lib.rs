extern crate proc_macro;

mod parse;
mod replace;

use crate::parse::ReductionArgs;
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
    let last_expr = if let Some(stmt) = exec_fn.stmts.pop() {
        let last_stmt = replace_lamellar_dsl(stmt.clone());
        match am_type {
            AmType::NoReturn => quote_spanned! {last_stmt.span()=>
                #last_stmt
                #lamellar::LamellarReturn::Unit
            },
            AmType::ReturnData(_) | AmType::ReturnAm(_) => {
                let temp = get_expr(&last_stmt)
                    .expect("failed to get exec return value (try removing the last \";\")");
                quote_spanned! {last_stmt.span()=> #temp}
            }
        }
    } else {
        quote_spanned! {func_span=> #lamellar::LamellarReturn::Unit }
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
                    #lamellar::serialized_size(self)
                }
                fn serialize_into(&self,buf: &mut [u8]){
                    // println!("buf_len: {:?} serialized size: {:?}",buf.len(), #lamellar::serialized_size(self));
                    #lamellar::serialize_into(buf,self).unwrap();
                }
            }

            impl #impl_generics #lamellar::LamellarResultSerde for #orig_name #ty_generics #where_clause {
                fn serialized_result_size(&self,result: &Box<dyn std::any::Any + Send + Sync>)->usize{
                    let result  = result.downcast_ref::<#ret_type>().unwrap();
                    #lamellar::serialized_size(result)
                }
                fn serialize_result_into(&self,buf: &mut [u8],result: &Box<dyn std::any::Any + Send + Sync>){
                    let result  = result.downcast_ref::<#ret_type>().unwrap();
                    #lamellar::serialize_into(buf,result).unwrap();
                }
            }
        });
    }

    let ret_statement = match am_type {
        AmType::NoReturn => quote! {
            #last_expr
        },
        AmType::ReturnData(_) => {
            quote! {
                let ret = match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                    true => #lamellar::LamellarReturn::LocalData(Box::new(#last_expr)),
                    false => #lamellar::LamellarReturn::RemoteData(std::sync::Arc::new (#ret_struct{
                        val: #last_expr
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
        expanded.extend(quote_spanned! {temp.span()=>
            struct #ret_struct{
                val: #ret_type
            }

            impl #generics #lamellar::LamellarSerde for #ret_struct {
                fn serialized_size(&self)->usize{
                    #lamellar::serialized_size(&self.val)
                }
                fn serialize_into(&self,buf: &mut [u8]){
                    #lamellar::serialize_into(buf,&self.val).unwrap();
                }
            }
        });
    }

    if !local {
        expanded.extend( quote_spanned! {temp.span()=>
            impl #impl_generics #lamellar::RemoteActiveMessage for #orig_name #ty_generics #where_clause {}
            fn #orig_name_unpack #impl_generics (bytes: &[u8], cur_pe: Result<usize,#lamellar::IdError>) -> std::sync::Arc<dyn #lamellar::RemoteActiveMessage + Send + Sync>  {
                let __lamellar_data: std::sync::Arc<#orig_name #ty_generics> = std::sync::Arc::new(#lamellar::deserialize(&bytes).unwrap());
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

        let mut fields = quote! {};
        let mut ser = quote! {};
        let mut des = quote! {};

        for field in &data.fields {
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

fn create_reduction(
    typeident: syn::Ident,
    reduction: String,
    op: proc_macro2::TokenStream,
    array_types: &Vec<syn::Ident>,
    rt: bool,
) -> proc_macro2::TokenStream {
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let (am_data, am): (syn::Path, syn::Path) = if rt {
        (
            syn::parse("lamellar_impl::AmDataRT".parse().unwrap()).unwrap(),
            syn::parse("lamellar_impl::rt_am".parse().unwrap()).unwrap(),
        )
    } else {
        (
            syn::parse("lamellar::AmData".parse().unwrap()).unwrap(),
            syn::parse("lamellar::am".parse().unwrap()).unwrap(),
        )
    };
    let reduction = quote::format_ident!("{:}", reduction);
    let reduction_gen = quote::format_ident!("{:}_{:}_reduction_gen", typeident, reduction);

    let mut gen_match_stmts = quote! {};
    let mut array_impls = quote! {};

    for array_type in array_types {
        let reduction_name =
            quote::format_ident!("{:}_{:}_{:}_reduction", array_type, typeident, reduction);

        gen_match_stmts.extend(quote!{
            #lamellar::array::LamellarByteArray::#array_type(inner) => std::sync::Arc::new(#reduction_name{
                data: unsafe {inner.clone().into()} , start_pe: 0, end_pe: num_pes-1}),
        });

        let iter_chain = if array_type == "AtomicArray"{
            quote!{.map(|elem| elem.load())}
        }
        else{
            quote!{.copied()}
        };

        array_impls.extend(quote!{
            #[allow(non_camel_case_types)]
            #[#am_data(Clone)]
            struct #reduction_name{
                data: #lamellar::array::#array_type<#typeident>,
                start_pe: usize,
                end_pe: usize,
            }

            #[#am]
            impl LamellarAM for #reduction_name{
                fn exec(&self) -> #typeident{
                    if self.start_pe == self.end_pe{
                        // println!("[{:?}] root {:?} {:?}",__lamellar_current_pe,self.start_pe, self.end_pe);
                        let timer = std::time::Instant::now();
                        let data_slice = unsafe {self.data.local_data()};
                        // println!("data: {:?}",data_slice);
                        // let first = data_slice.first().unwrap().clone();
                        // let res = data_slice[1..].iter().fold(first, #op );
                        let res = data_slice.iter()#iter_chain.reduce(#op).unwrap();
                        // println!("[{:?}] {:?} {:?}",__lamellar_current_pe,res,timer.elapsed().as_secs_f64());
                        res
                        // data_slice.first().unwrap().clone()
                    }
                    else{
                        // println!("[{:?}] recurse {:?} {:?}",__lamellar_current_pe,self.start_pe, self.end_pe);
                        let mid_pe = (self.start_pe + self.end_pe)/2;
                        let op = #op;
                        let timer = std::time::Instant::now();
                        let left = __lamellar_team.exec_am_pe( self.start_pe,  #reduction_name { data: self.data.clone(), start_pe: self.start_pe, end_pe: mid_pe}).into_future();
                        let right = __lamellar_team.exec_am_pe( mid_pe+1, #reduction_name { data: self.data.clone(), start_pe: mid_pe+1, end_pe: self.end_pe}).into_future();
                        let res = op(left.await.unwrap(),right.await.unwrap());

                        // println!("[{:?}] {:?} {:?}",__lamellar_current_pe,res,timer.elapsed().as_secs_f64());
                        res
                        // let data_slice = unsafe {self.data.local_data()};
                        // data_slice.first().unwrap().clone()
                    }
                }
            }
        });
    }

    let expanded = quote! {
        fn  #reduction_gen (data: #lamellar::array::LamellarByteArray, num_pes: usize)
        -> std::sync::Arc<dyn #lamellar::RemoteActiveMessage + Send + Sync >{
            match data{
                #gen_match_stmts
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

        #array_impls
    };

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            use __lamellar::array::{LamellarArrayPut};
            #expanded
        };
    };
    if lamellar == "crate" {
        expanded
    } else {
        user_expanded
    }
}

fn gen_array_impls(
    typeident: syn::Ident,
    array_types: &Vec<(syn::Ident, syn::Ident)>,
    ops: &Vec<(syn::Ident, bool)>,
    ops_type: OpType,
    rt: bool,
) -> proc_macro2::TokenStream {
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let (am_data, am): (syn::Path, syn::Path) = if rt {
        (
            syn::parse("lamellar_impl::AmDataRT".parse().unwrap()).unwrap(),
            syn::parse("lamellar_impl::rt_am".parse().unwrap()).unwrap(),
        )
    } else {
        (
            syn::parse("lamellar::AmData".parse().unwrap()).unwrap(),
            syn::parse("lamellar::am".parse().unwrap()).unwrap(),
        )
    };

    let mut array_impls = quote! {};

    array_impls.extend(quote! {
        #[allow(non_camel_case_types)]
    });

    for (array_type, byte_array_type) in array_types {
        let create_ops = match ops_type {
            OpType::Arithmetic => quote::format_ident!("{}_create_ops", array_type),
            OpType::Bitwise => quote::format_ident!("{}_create_bitwise_ops", array_type),
            OpType::Atomic => quote::format_ident!("{}_create_atomic_ops", array_type),
        };
        let op_fn_name_base = quote::format_ident!("{}_{}_", array_type, typeident);
        array_impls.extend(quote! {
            #lamellar::#create_ops!(#typeident,#op_fn_name_base);
        });
        for (op, fetch) in ops.clone().into_iter() {
            let op_am_name = quote::format_ident!("{}_{}_{}_am", array_type, typeident, op);
            let dist_fn_name = quote::format_ident!("{}dist_{}", op_fn_name_base, op);
            let local_op = quote::format_ident!("local_{}", op);
            array_impls.extend(quote! {
                #[#am_data]
                struct #op_am_name{
                    data: #lamellar::array::#array_type<#typeident>,
                    local_index: usize,
                    val: #typeident,
                }
            });

            let exec = match fetch {
                true => quote! {
                    fn exec(&self) -> #typeident {
                        self.data.#local_op(self.local_index,self.val)
                    }
                },
                false => quote! {
                    fn exec(&self) {
                        self.data.#local_op(self.local_index,self.val);
                    }
                },
            };
            array_impls.extend(quote!{
                #[#am]
                impl LamellarAM for #op_am_name{
                    #exec
                }
                #[allow(non_snake_case)]
                fn #dist_fn_name(val: *const u8, array: #lamellar::array::#byte_array_type, index: usize) -> Arc<dyn RemoteActiveMessage + Send + Sync>{
                    // println!("{:}",stringify!(#dist_fn_name));
                    let val = unsafe {*(val as  *const #typeident)};
                    Arc::new(#op_am_name{
                        data: unsafe {array.into()},
                        local_index: index,
                        val: val,
                    })
                }
            });
        }
    }
    array_impls
}

#[cfg(feature="non-buffered-array-ops")]
fn gen_write_array_impls(
    typeident: syn::Ident,
    array_types: &Vec<(syn::Ident, syn::Ident)>,
    ops: &Vec<(syn::Ident, bool)>,
    rt: bool,
) -> proc_macro2::TokenStream {
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let mut write_array_impl = quote! {};
    for (op, fetch) in ops.clone().into_iter() {
        let mut match_stmts = quote! {};
        for (array_type, _) in array_types {
            match_stmts.extend(quote! {
                #lamellar::array::LamellarWriteArray::#array_type(inner) => inner.#op(index,val),
            });
        }
        let return_type = match fetch {
            true => {
                quote! { Box<dyn #lamellar::LamellarRequest<Output = #typeident> + Send + Sync> }
            }
            false => {
                quote! {Option<Box<dyn #lamellar::LamellarRequest<Output = ()> + Send + Sync>>}
            }
        };
        write_array_impl.extend(quote! {
            fn #op(&self,index: usize, val: #typeident)-> #return_type{
                match self{
                    #match_stmts
                }
            }
        });
    }
    write_array_impl
}


fn create_buf_ops( typeident: syn::Ident, array_type: syn::Ident, byte_array_type: syn::Ident,optypes: &Vec<OpType>,rt: bool,bitwise: bool) -> proc_macro2::TokenStream {
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let (am_data, am): (syn::Path, syn::Path) = if rt {
        (
            syn::parse("lamellar_impl::AmDataRT".parse().unwrap()).unwrap(),
            syn::parse("lamellar_impl::rt_am".parse().unwrap()).unwrap(),
        )
    } else {
        (
            syn::parse("lamellar::AmData".parse().unwrap()).unwrap(),
            syn::parse("lamellar::am".parse().unwrap()).unwrap(),
        )
    };
    let mut expanded = quote!{};
    let (lhs,assign,load) = if array_type == "AtomicArray"{
        let array_vec =  vec![(array_type.clone(),byte_array_type.clone())];
        let ops: Vec<(syn::Ident, bool)> = vec![
            (quote::format_ident!("add"), false),
            (quote::format_ident!("fetch_add"), true),
            (quote::format_ident!("sub"), false),
            (quote::format_ident!("fetch_sub"), true),
            (quote::format_ident!("mul"), false),
            (quote::format_ident!("fetch_mul"), true),
            (quote::format_ident!("div"), false),
            (quote::format_ident!("fetch_div"), true),
        ]; 
        let temp = gen_array_impls(
            typeident.clone(),
            &array_vec,
            &ops,
            OpType::Arithmetic,
            rt,
        );
        expanded.extend(quote!{#temp});
        let ops: Vec<(syn::Ident, bool)> = vec![
            (quote::format_ident!("load"), true),
            (quote::format_ident!("store"), false),
            (quote::format_ident!("swap"), true),
        ]; 
        let temp = gen_array_impls(
            typeident.clone(),
            &array_vec,
            &ops,
            OpType::Atomic,
            rt,
        );
        expanded.extend(quote!{#temp});
        if bitwise{
            let ops: Vec<(syn::Ident, bool)> = vec![
                (quote::format_ident!("bit_and"), false),
                (quote::format_ident!("fetch_bit_and"), true),
                (quote::format_ident!("bit_or"), false),
                (quote::format_ident!("fetch_bit_or"), true),
            ]; 
            let temp = gen_array_impls(
                typeident.clone(),
                &array_vec,
                &ops,
                OpType::Bitwise,
                rt,
            );
            expanded.extend(quote!{#temp});
        }
        (quote!{let mut elem = slice.at(index); elem },
        quote!{slice.at(index).store(val)},
        quote!{slice.at(index).load()})
    }
    else{
        (quote!{slice[index]},
        quote!{slice[index] = val},
        quote!{slice[index]})
    };

    
    let mut match_stmts = quote! {};
    for optype in optypes{
        match optype{
            OpType::Arithmetic =>{
                match_stmts.extend(quote! {
                    ArrayOpCmd::Add=>{ #lhs += val },
                    ArrayOpCmd::FetchAdd=> { 
                        results_slice[fetch_index] = orig;
                        fetch_index+=1;
                        #lhs += val
                    },
                    ArrayOpCmd::Sub=>{#lhs -= val},
                    ArrayOpCmd::FetchSub=>{ 
                        results_slice[fetch_index] = orig;
                        fetch_index+=1;
                        #lhs -= val
                    },
                    ArrayOpCmd::Mul=>{#lhs *= val},
                    ArrayOpCmd::FetchMul=>{ 
                        results_slice[fetch_index] = orig;
                        fetch_index+=1;
                        #lhs *= val
                    },
                    ArrayOpCmd::Div=>{#lhs /= val},
                    ArrayOpCmd::FetchDiv=>{ 
                        results_slice[fetch_index] = orig;
                        fetch_index+=1;
                        #lhs /= val
                    },
                })
            },
            OpType::Bitwise =>{
                match_stmts.extend(quote! {
                    ArrayOpCmd::And=>{#lhs &= val},
                    ArrayOpCmd::FetchAnd=>{ 
                        results_slice[fetch_index] = orig;
                        fetch_index+=1;
                        #lhs &= val
                    },
                    ArrayOpCmd::Or=>{#lhs |= val},
                    ArrayOpCmd::FetchOr=>{ 
                        results_slice[fetch_index] = orig;
                        fetch_index+=1;
                        #lhs |= val
                    },
                })
            },
            OpType::Atomic =>{
                match_stmts.extend(quote! {
                    ArrayOpCmd::Store=>{#assign},
                    ArrayOpCmd::Load=>{ 
                        results_slice[fetch_index] = orig;
                        fetch_index+=1;
                    },
                    ArrayOpCmd::Swap=>{ 
                        results_slice[fetch_index] = orig;
                        fetch_index+=1;
                        #assign
                    },
                })
            },
        }
    }

    let buf_op_name = quote::format_ident!("{}_{}_op_buf", array_type, typeident);
    let am_buf_name = quote::format_ident!("{}_{}_am_buf", array_type, typeident);
    let dist_am_buf_name = quote::format_ident!("{}_{}_am_buf", array_type, typeident);
    let reg_name = quote::format_ident!("{}OpBuf", array_type);
    
    expanded.extend(quote! {
        struct #buf_op_name{
            data: #lamellar::array::#array_type<#typeident>,
            ops: Mutex<Vec<(ArrayOpCmd,usize,#typeident)>>,
            complete: RwLock<Arc<AtomicBool>>,
            result_cnt:  RwLock<Arc<AtomicUsize>>,
            results:  RwLock<Arc<RwLock<Vec<u8>>>>,
        }
        #[#am_data]
        struct #am_buf_name{
            data: #lamellar::array::#array_type<#typeident>,
            ops: Vec<(ArrayOpCmd,usize,#typeident)>, 
            num_fetch_ops: usize,           
        }
        impl #lamellar::array::BufferOp for #buf_op_name{
            fn add_op(&self, op: ArrayOpCmd, index: usize, val: *const u8) -> (usize,Arc<AtomicBool>){
                let val = unsafe{*(val as *const #typeident)};
                let mut buf = self.ops.lock();
                buf.push((op,index,val));
                (buf.len(),self.complete.read().clone())
            }
            fn add_fetch_op(&self, op: ArrayOpCmd, index: usize, val: *const u8) -> (usize,Arc<AtomicBool>, usize,Arc<RwLock<Vec<u8>>>){
                let val = unsafe{*(val as *const #typeident)};
                let mut buf = self.ops.lock();
                buf.push((op,index,val));
                let res_index = self.result_cnt.read().fetch_add(1,Ordering::SeqCst);

                (buf.len(),self.complete.read().clone(),res_index,self.results.read().clone())
            }
            fn into_arc_am(&self,sub_array: std::ops::Range<usize>) -> (Arc<dyn RemoteActiveMessage + Send + Sync>,usize,Arc<AtomicBool>,Arc<RwLock<Vec<u8>>>){
                
                let mut buf = self.ops.lock();
                let mut am = #am_buf_name{
                    data: self.data.sub_array(sub_array),
                    ops: Vec::new(),
                    num_fetch_ops: self.result_cnt.read().swap(0,Ordering::SeqCst),
                };
                let len = buf.len();
                std::mem::swap(&mut am.ops,  &mut buf);
                let mut complete = Arc::new(AtomicBool::new(false));
                std::mem::swap(&mut complete, &mut self.complete.write());
                let mut results = Arc::new(RwLock::new(Vec::new()));
                std::mem::swap(&mut results, &mut self.results.write());
                (Arc::new(am),len,complete,results)
            }
        }
        #[#am]
        impl LamellarAM for #am_buf_name{ //eventually we can return fetchs here too...
            fn exec(&self) -> Vec<u8>{ 
                // self.data.process_ops(&self.ops);
                // println!("num ops {:?} ",self.ops.len());
                let mut slice = unsafe{self.data.mut_local_data()};
                // println!("slice len {:?}",slice.len());
                let u8_len = self.num_fetch_ops*std::mem::size_of::<#typeident>();
                let mut results_u8: Vec<u8> = if u8_len > 0 {
                    let mut temp = Vec::with_capacity(u8_len);
                    unsafe{temp.set_len(u8_len)};
                    temp
                }
                else{
                    vec![]
                };
                let mut results_slice = unsafe{std::slice::from_raw_parts_mut(results_u8.as_mut_ptr() as *mut #typeident,self.num_fetch_ops)};
                let mut fetch_index=0;
                for (op,index,val) in &self.ops{
                    // println!("op: {:?} index{:?} val {:?}",op,index,val);
                    let index = *index;
                    let val = *val;
                    let orig = #load;
                    match op{
                    # match_stmts
                    _ => {panic!("shouldnt happen {:?}",op)}
                    }
                }
                // println!("buff ops exec: {:?} {:?} {:?}",results_u8.len(),u8_len,self.num_fetch_ops);
                results_u8
            }
        }
        #[allow(non_snake_case)]
        fn #dist_am_buf_name(array: #lamellar::array::#byte_array_type) -> Arc<dyn #lamellar::array::BufferOp>{
            // println!("{:}",stringify!(#dist_fn_name));
            Arc::new(#buf_op_name{
                data: unsafe {array.into()},
                ops: Mutex::new(Vec::new()),
                complete: RwLock::new(Arc::new(AtomicBool::new(false))),
                result_cnt: RwLock::new(Arc::new(AtomicUsize::new(0))),
                results: RwLock::new( Arc::new(RwLock::new(Vec::new()))),
            })
        }
        inventory::submit! {
            #![crate = #lamellar]
            #lamellar::array::#reg_name{
                id: std::any::TypeId::of::<#typeident>(),
                op: #dist_am_buf_name,
            }
        }
    });
    expanded
}

enum OpType {
    Arithmetic,
    Bitwise,
    Atomic,
}

fn create_buffered_ops(typeident: syn::Ident, bitwise: bool, rt: bool) -> proc_macro2::TokenStream {
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let atomic_array_types: Vec<(syn::Ident, syn::Ident)> = vec![
        (
            quote::format_ident!("CollectiveAtomicArray"),
            quote::format_ident!("CollectiveAtomicByteArray"),
        ),
        (
            quote::format_ident!("AtomicArray"),
            quote::format_ident!("AtomicByteArray"),
        )
    ];

    let mut expanded = quote! {};
    
    let mut optypes = vec![OpType::Arithmetic];
    if bitwise{
        optypes.push(OpType::Bitwise);
    }
    let buf_op_impl = create_buf_ops( typeident.clone(), 
        quote::format_ident!("UnsafeArray"),
        quote::format_ident!("UnsafeByteArray"),
    &optypes,rt,bitwise);
    expanded.extend(buf_op_impl);
    optypes.push(OpType::Atomic);
    for (array_type,byte_array_type) in atomic_array_types{
        let buf_op_impl = create_buf_ops( typeident.clone(), array_type.clone(),byte_array_type.clone(),&optypes,rt,bitwise);
        expanded.extend(buf_op_impl)
    }    

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            use __lamellar::array::{AtomicArray,AtomicByteArray,CollectiveAtomicArray,CollectiveAtomicByteArray,LocalArithmeticOps,LocalAtomicOps,ArrayOpCmd,LamellarArrayPut};
            use __lamellar::array;
            // #bitwise_mod
            use __lamellar::LamellarArray;
            use __lamellar::LamellarRequest;
            use __lamellar::RemoteActiveMessage;
            use std::sync::Arc;
            use parking_lot::{Mutex,RwLock};
            use std::sync::atomic::{Ordering,AtomicBool,AtomicUsize};
            #expanded
        };
    };
    if lamellar == "crate" {
        expanded
    } else {
        user_expanded
    }
}

#[cfg(feature="non-buffered-array-ops")]
fn create_ops(typeident: syn::Ident, bitwise: bool, rt: bool) -> proc_macro2::TokenStream {
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let write_array_types: Vec<(syn::Ident, syn::Ident)> = vec![
        (
            quote::format_ident!("CollectiveAtomicArray"),
            quote::format_ident!("CollectiveAtomicByteArray"),
        ),
        (
            quote::format_ident!("AtomicArray"),
            quote::format_ident!("AtomicByteArray"),
        ),
        (
            quote::format_ident!("UnsafeArray"),
            quote::format_ident!("UnsafeByteArray"),
        ),
    ];
    let ops: Vec<(syn::Ident, bool)> = vec![
        (quote::format_ident!("add"), false),
        (quote::format_ident!("fetch_add"), true),
        (quote::format_ident!("sub"), false),
        (quote::format_ident!("fetch_sub"), true),
        (quote::format_ident!("mul"), false),
        (quote::format_ident!("fetch_mul"), true),
        (quote::format_ident!("div"), false),
        (quote::format_ident!("fetch_div"), true),
    ]; 

    let array_impls = gen_array_impls(
        typeident.clone(),
        &write_array_types,
        &ops,
        OpType::Arithmetic,
        rt,
    );
    let write_array_impls = gen_write_array_impls(typeident.clone(), &write_array_types, &ops, rt);
    let mut expanded = quote! {
        #[allow(non_camel_case_types)]
        impl #lamellar::array::ArithmeticOps<#typeident> for #lamellar::array::LamellarWriteArray<#typeident>{
            #write_array_impls
        }
        #array_impls
    };

    let atomic_array_types: Vec<(syn::Ident, syn::Ident)> = vec![
        (
            quote::format_ident!("CollectiveAtomicArray"),
            quote::format_ident!("CollectiveAtomicByteArray"),
        ),
        (
            quote::format_ident!("AtomicArray"),
            quote::format_ident!("AtomicByteArray"),
        ),
    ];
    let atomic_ops: Vec<(syn::Ident, bool)> = vec![
        (quote::format_ident!("load"), true),
        (quote::format_ident!("store"), false),
        (quote::format_ident!("swap"), true),
    ];
    let array_impls = gen_array_impls(
        typeident.clone(),
        &atomic_array_types,
        &atomic_ops,
        OpType::Atomic,
        rt,
    );
    expanded.extend(quote! {
        #array_impls
    }); 

    let mut bitwise_mod = quote! {};
    if bitwise {
        let bitwise_ops: Vec<(syn::Ident, bool)> = vec![
            (quote::format_ident!("bit_and"), false),
            (quote::format_ident!("fetch_bit_and"), true),
            (quote::format_ident!("bit_or"), false),
            (quote::format_ident!("fetch_bit_or"), true),
        ];

        // let (array_impls,write_array_impl) = gen_op_impls(typeident.clone(),array_types,&bitwise_ops,true,rt);
        let array_impls = gen_array_impls(
            typeident.clone(),
            &write_array_types,
            &bitwise_ops,
            OpType::Bitwise,
            rt,
        );
        let write_array_impls =
            gen_write_array_impls(typeident.clone(), &write_array_types, &bitwise_ops, rt);
        expanded.extend(quote!{
            #[allow(non_camel_case_types)]
            impl #lamellar::array::BitWiseOps<#typeident> for #lamellar::array::LamellarWriteArray<#typeident>{
                #write_array_impls
            }
            #array_impls
        });
        bitwise_mod.extend(quote! {use __lamellar::array::LocalBitWiseOps;});
    }

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            use __lamellar::array::{AtomicArray,AtomicByteArray,CollectiveAtomicArray,CollectiveAtomicByteArray,LocalArithmeticOps,LocalAtomicOps,ArrayOpCmd,LamellarArrayPut};
            #bitwise_mod
            use __lamellar::LamellarArray;
            use __lamellar::LamellarRequest;
            use __lamellar::RemoteActiveMessage;
            use std::sync::Arc;
            use parking_lot::Mutex;
            #expanded
        };
    };
    if lamellar == "crate" {
        expanded
    } else {
        user_expanded
    }
}

fn gen_atomic_rdma(typeident: syn::Ident, rt: bool) -> proc_macro2::TokenStream {
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let (am_data, am): (syn::Path, syn::Path) = if rt {
        (
            syn::parse("lamellar_impl::AmDataRT".parse().unwrap()).unwrap(),
            syn::parse("lamellar_impl::rt_am".parse().unwrap()).unwrap(),
        )
    } else {
        (
            syn::parse("lamellar::AmData".parse().unwrap()).unwrap(),
            syn::parse("lamellar::am".parse().unwrap()).unwrap(),
        )
    };

    let get_name = quote::format_ident!("RemoteGetAm_{}", typeident);
    let get_fn = quote::format_ident!("RemoteGetAm_{}_gen", typeident);
    let put_name = quote::format_ident!("RemotePutAm_{}", typeident);
    let put_fn = quote::format_ident!("RemotePutAm_{}_gen", typeident);
    quote! {
        // #[allow(non_snake_case)]
        #[#am_data]
        struct #get_name {
            array: #lamellar::array::AtomicByteArray, //inner of the indices we need to place data into
            start_index: usize,
            len: usize,
        }

        #[#am]
        impl LamellarAm for #get_name {
            fn exec(self) -> Vec<u8> {
                // println!("in remotegetam {:?} {:?}",self.start_index,self.len);
                let array: #lamellar::array::AtomicArray<#typeident> =unsafe {self.array.clone().into()};
                unsafe {
                    match array.array.local_elements_for_range(self.start_index,self.len){
                        Some((unsafe_u8_elems,indices)) => {
                            let unsafe_elems = std::slice::from_raw_parts(unsafe_u8_elems.as_ptr() as *const #typeident,unsafe_u8_elems.len()/std::mem::size_of::<#typeident>());
                            let elems_u8_len = unsafe_elems.len() * std::mem::size_of::<#typeident>();
                            let mut elems_u8: Vec<u8> = Vec::with_capacity(elems_u8_len);
                            elems_u8.set_len(elems_u8_len);
                            let elems_t_ptr = elems_u8.as_mut_ptr() as *mut #typeident;
                            let elems_t_len = unsafe_elems.len();
                            let elems = std::slice::from_raw_parts_mut(elems_t_ptr,elems_t_len);
                            for (buf_i,array_i) in indices.enumerate(){
                                // println!("i {:?} {:?}",(buf_i,array_i),unsafe_elems[buf_i]);
                                elems[buf_i] = array.local_load(array_i,unsafe_elems[buf_i]);
                            }

                            // println!("{:?} {:?} {:?}",unsafe_elems,elems,elems_u8);
                            // println!("{:?}",elems_u8);

                            elems_u8
                        },
                        None => vec![],
                    }
                }
            }
        }

        #[allow(non_snake_case)]
        fn #get_fn(array: #lamellar::array::AtomicByteArray, start_index: usize, len: usize) -> Arc<dyn RemoteActiveMessage + Send + Sync>{
            Arc::new(#get_name{
                array: array,
                start_index: start_index,
                len: len,
            })
        }

        #lamellar::inventory::submit! {
            #![crate = #lamellar]
            #lamellar::array::atomic::rdma::AtomicArrayGet{
                id: std::any::TypeId::of::<#typeident>(),
                op: #get_fn
            }
        }

        #[#am_data]
        struct #put_name {
            array: #lamellar::array::AtomicByteArray, //inner of the indices we need to place data into
            start_index: usize,
            len: usize,
            buf : Vec<u8>,
        }

        #[#am]
        impl LamellarAm for #put_name {
            fn exec(self) {
                // println!("in remoteput {:?} {:?}",self.start_index,self.len);
                let array: #lamellar::array::AtomicArray<#typeident> =unsafe {self.array.clone().into()};

                unsafe {
                    let buf = std::slice::from_raw_parts(self.buf.as_ptr() as *const #typeident,self.buf.len() / std::mem::size_of::<#typeident>());
                    // println!("buf: {:?}",buf);
                    match array.array.local_elements_for_range(self.start_index,self.len){
                        Some((elems,indices)) => {
                            // println!("elems {:?}",elems);
                            for (buf_i,array_i) in indices.enumerate(){
                                // println!("buf_i {:?} array_i {:?}",buf_i,array_i);
                                array.local_store(array_i,buf[buf_i]);
                            }
                        },
                        None => {},
                    }
                }
                // println!("done remoteput");
            }
        }

        #[allow(non_snake_case)]
        fn #put_fn(array: #lamellar::array::AtomicByteArray, start_index: usize, len: usize, buf: Vec<u8>) -> Arc<dyn RemoteActiveMessage + Send + Sync>{
            Arc::new(#put_name{
                array: array,
                start_index: start_index,
                len: len,
                buf: buf,
            })
        }

        #lamellar::inventory::submit! {
            #![crate = #lamellar]
            #lamellar::array::atomic::rdma::AtomicArrayPut{
                id: std::any::TypeId::of::<#typeident>(),
                op: #put_fn
            }
        }
    }
}

#[proc_macro_error]
#[proc_macro]
pub fn register_reduction(item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(item as ReductionArgs);
    let mut output = quote! {};
    let array_types: Vec<syn::Ident> = vec![
        quote::format_ident!("CollectiveAtomicArray"),
        quote::format_ident!("AtomicArray"),
        quote::format_ident!("UnsafeArray"),
        quote::format_ident!("ReadOnlyArray"),
    ];

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
            // let tyr: syn::TypeReference = syn::parse_quote! {&#tyc};
            let tyc = ty.clone();
            let pat: syn::PatType = syn::PatType {
                attrs: vec![],
                pat: Box::new(syn::Pat::Ident(b.clone())),
                colon_token: syn::Token![:](Span::call_site()),
                // ty: Box::new(syn::Type::Reference(tyr)),
                ty: Box::new(syn::Type::Path(tyc.clone())),
            };
            closure.inputs[1] = syn::Pat::Type(pat);
        }
        println!("{:?}", closure);

        output.extend(create_reduction(
            ty.path.segments[0].ident.clone(),
            args.name.to_string(),
            quote! {#closure},
            &array_types,
            false,
            // "lamellar".to_string(),
        ));
    }
    TokenStream::from(output)
}

#[proc_macro_error]
#[proc_macro]
pub fn generate_reductions_for_type(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    let read_array_types: Vec<syn::Ident> = vec![
        quote::format_ident!("CollectiveAtomicArray"),
        quote::format_ident!("AtomicArray"),
        quote::format_ident!("UnsafeArray"),
        quote::format_ident!("ReadOnlyArray"),
    ];

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
            &read_array_types,
            false,
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "prod".to_string(),
            quote! {
                let data_slice = <lamellar::LamellarMemoryRegion<#typeident> as lamellar::RegisteredMemoryRegion>::as_slice(&self.data).unwrap();
                let first = data_slice.first().unwrap().clone();
                data_slice[1..].iter().fold(first,|acc,val|{ acc*val } )
            },
            &read_array_types,
            false,
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "max".to_string(),
            quote! {
                *<lamellar::LamellarMemoryRegion<#typeident> as lamellar::RegisteredMemoryRegion>::as_slice(&self.data).unwrap().iter().max().unwrap()
            },
            &read_array_types,
            false,
        ));
    }

    TokenStream::from(output)
}
#[proc_macro_error]
#[proc_macro]
pub fn generate_reductions_for_type_rt(item: TokenStream) -> TokenStream {
    let mut output = quote! {};

    let read_array_types: Vec<syn::Ident> = vec![
        quote::format_ident!("CollectiveAtomicArray"),
        quote::format_ident!("AtomicArray"),
        quote::format_ident!("UnsafeArray"),
        quote::format_ident!("ReadOnlyArray"),
    ];

    for t in item.to_string().split(",").collect::<Vec<&str>>() {
        let t = t.trim().to_string();
        let typeident = quote::format_ident!("{:}", t.clone());
        // let elemtypeident = if 
        output.extend(create_reduction(
            typeident.clone(),
            "sum".to_string(),
            quote! {
                |acc, val|{ acc + val }
            },
            &read_array_types,
            true,
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "prod".to_string(),
            quote! {
                |acc, val| { acc * val }
            },
            &read_array_types,
            true,
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "max".to_string(),
            quote! {
                |val1, val2| { if val1 > val2 {val1} else {val2} }
            },
            &read_array_types,
            true,
        ));
    }
    TokenStream::from(output)
}

#[proc_macro_error]
#[proc_macro]
pub fn generate_ops_for_type(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    let items = item
        .to_string()
        .split(",")
        .map(|i| i.to_owned())
        .collect::<Vec<String>>();
    let bitwise = if let Ok(val) = syn::parse_str::<syn::LitBool>(&items[0]) {
        val.value
    } else {
        panic! ("first argument of generate_ops_for_type expects 'true' or 'false' specifying whether type implements bitwise operations");
    };
    for t in items[1..].iter() {
        let typeident = quote::format_ident!("{:}", t.trim());
        output.extend(quote! {impl Dist for #typeident {}});
        #[cfg(feature="non-buffered-array-ops")]
        output.extend(create_ops(typeident.clone(), bitwise, false));
        #[cfg(not(feature="non-buffered-array-ops"))]
        output.extend(create_buffered_ops(typeident.clone(), bitwise, false));
        output.extend(gen_atomic_rdma(typeident.clone(), false));
    }
    TokenStream::from(output)
}

#[proc_macro_error]
#[proc_macro]
pub fn generate_ops_for_type_rt(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    let items = item
        .to_string()
        .split(",")
        .map(|i| i.to_owned())
        .collect::<Vec<String>>();
    let bitwise = if let Ok(val) = syn::parse_str::<syn::LitBool>(&items[0]) {
        val.value
    } else {
        panic! ("first argument of generate_ops_for_type expects 'true' or 'false' specifying whether type implements bitwise operations");
    };
    for t in items[1..].iter() {
        let typeident = quote::format_ident!("{:}", t.trim());
        output.extend(quote! {impl Dist for #typeident {}});
        #[cfg(feature="non-buffered-array-ops")]
        output.extend(create_ops(typeident.clone(), bitwise, true));
        #[cfg(not(feature="non-buffered-array-ops"))]
        output.extend(create_buffered_ops(typeident.clone(), bitwise, true));
        output.extend(gen_atomic_rdma(typeident.clone(), true));
    }
    TokenStream::from(output)
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
#[proc_macro_derive(ArithmeticOps)]
pub fn derive_arrayops(input: TokenStream) -> TokenStream {
    let mut output = quote! {};

    let input = parse_macro_input!(input as syn::DeriveInput);
    let name = input.ident;

    #[cfg(feature="non-buffered-array-ops")]
    output.extend(create_ops(name.clone(), false, false));
    #[cfg(not(feature="non-buffered-array-ops"))]
    output.extend(create_buffered_ops(name.clone(), false, false));
    TokenStream::from(output)
}

#[proc_macro_error]
#[proc_macro_derive(AtomicRdma)]
pub fn derive_atomicrdma(input: TokenStream) -> TokenStream {
    let mut output = quote! {};

    let input = parse_macro_input!(input as syn::DeriveInput);
    let name = input.ident;

    output.extend(gen_atomic_rdma(name.clone(), true));
    TokenStream::from(output)
}
