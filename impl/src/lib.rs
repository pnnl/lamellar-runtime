extern crate proc_macro;

mod parse;
mod replace;

use crate::parse::{ReductionArgs};
use crate::replace::{SelfReplace,LamellarDSLReplace};
// use crate::local_am::local_am_without_return;

use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_error::{proc_macro_error};
use quote::{quote,quote_spanned};
use syn::{parse_macro_input};
use syn::visit_mut::VisitMut;
use syn::spanned::Spanned;



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

enum AmType{
    NoReturn,
    ReturnData(syn::Type),
    ReturnAm(proc_macro2::TokenStream),
}

fn get_return_am_return_type(args: String) -> proc_macro2::TokenStream{
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
        let ident = syn::Ident::new(&return_type, Span::call_site());
        quote!{#ident}
    }else {
        quote!{()}
    }
    
}

fn generate_am(input: syn::ItemImpl, local: bool, rt: bool, am_type: AmType) -> TokenStream {
    let name = type_name(&input.self_ty).expect("unable to find name");
    let lamellar = if rt {
        quote::format_ident!("crate")
    }
    else {
        quote::format_ident!("lamellar")
    };

    let generics = input.generics.clone();
    let generics_args = if let syn::Type::Path(ty) = *input.self_ty{
        if let Some(syn::PathSegment{ident:_,arguments}) = ty.path.segments.first(){
            arguments.clone()
        }else{
            syn::PathArguments::None
        }
    }
    else{
        syn::PathArguments::None
    };
    let mut exec_fn =
    get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
    
    let last_expr = if let Some(stmt) = exec_fn.stmts.pop(){
        let last_stmt = replace_lamellar_dsl(stmt.clone());
        match am_type{
            AmType::NoReturn => quote!{ 
                #last_stmt
                #lamellar::LamellarReturn::Unit 
            },
            AmType::ReturnData(_) | AmType::ReturnAm(_) => {
                let temp =get_expr(&last_stmt)
                .expect("failed to get exec return value (try removing the last \";\")");
                quote!{#temp}
            }
        }  
    }
    else{
        quote!{ #lamellar::LamellarReturn::Unit } 
    };
    // println!("last_expr {:?}",last_expr);
    
    let stmts = exec_fn.stmts;
    let mut temp = quote! {};

    for stmt in stmts {
        let new_stmt = replace_lamellar_dsl(stmt.clone());
        temp.extend(quote_spanned! {stmt.span()=>
            #new_stmt
        });
    }

    let orig_name = syn::Ident::new(&name, Span::call_site());
    let orig_name_unpack = quote::format_ident!("{}_unpack", orig_name.clone());
    

    let ret_type = {
        match am_type{
            AmType::NoReturn => quote!{()},
            AmType::ReturnData(ref output) => quote!{#output},
            AmType::ReturnAm(ref output) => {
                quote!{#output}
            }
        }
    };
    let ret_struct = quote::format_ident!("{}Result", orig_name);
    // println!("ret_type {:?}",ret_type);
    let mut am = quote!{
        impl #generics #lamellar::LocalAM for #orig_name#generics_args{
            type Output = #ret_type;
        }
    };
    if !local {
        am.extend(quote!{
            impl #generics #lamellar::LamellarAM for #orig_name#generics_args {
                type Output = #ret_type;
            }

            impl  #generics #lamellar::Serde for #orig_name#generics_args {}
        });
    }

    let ret_statement = match am_type{
        AmType::NoReturn => quote!{ 
            #last_expr 
        },
        AmType::ReturnData(_) => {
            quote!{
                let ret = match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                    true => #lamellar::LamellarReturn::LocalData(Box::new(#last_expr)),
                    false => #lamellar::LamellarReturn::RemoteData(std::sync::Arc::new (#ret_struct{    
                        val: #last_expr
                    })),
                };
                ret
            }
        },
        AmType::ReturnAm(_) => {
            quote!{
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


    let mut expanded = quote! {
        impl #generics #lamellar::LamellarActiveMessage for #orig_name#generics_args {
            fn exec(self: std::sync::Arc<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=#lamellar::LamellarReturn> + Send>>{
                Box::pin( async move {
                #temp
                #ret_statement
                })

            }
            fn get_id(&self) -> String{
                stringify!(#orig_name).to_string()
            }
        }

        
        impl #generics #lamellar::LamellarSerde for #orig_name#generics_args {
            fn serialized_size(&self)->usize{
                #lamellar::serialized_size(self)
            }
            fn serialize_into(&self,buf: &mut [u8]){
                #lamellar::serialize_into(buf,self).unwrap();
            }            
        }

        
        impl #generics #lamellar::LamellarResultSerde for #orig_name#generics_args {
            fn serialized_result_size(&self,result: &Box<dyn std::any::Any + Send + Sync>)->usize{
                let result  = result.downcast_ref::<#ret_type>().unwrap();
                #lamellar::serialized_size(result)
                
            }
            fn serialize_result_into(&self,buf: &mut [u8],result: &Box<dyn std::any::Any + Send + Sync>){
                let result  = result.downcast_ref::<#ret_type>().unwrap();
                #lamellar::serialize_into(buf,result).unwrap();
            }
        }

        #am
    };

    if let AmType::ReturnData(_) = am_type{
        expanded.extend(quote!{
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
        expanded.extend(quote!{
            fn #orig_name_unpack(bytes: &[u8]) -> std::sync::Arc<dyn #lamellar::LamellarActiveMessage + Send + Sync>  {
                let __lamellar_data: std::sync::Arc<#orig_name> = std::sync::Arc::new(#lamellar::deserialize(&bytes).unwrap());
                <#orig_name as #lamellar::DarcSerde>::des(&__lamellar_data);
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
    TokenStream::from(expanded)
}

fn derive_am_data(input: TokenStream,args: TokenStream, crate_header: String, local: bool) -> TokenStream{
    let lamellar = quote::format_ident!("{}", crate_header.clone());
    let input: syn::Item = parse_macro_input!(input);
    let mut output = quote!{};   

    if let syn::Item::Struct(data) = input{
        let name = &data.ident;
        let generics = data.generics.clone();
        let mut generics_ids=quote!{};
        for id in &generics.params{
            if let syn::GenericParam::Type(id) = id{
                let temp =id.ident.clone();
                generics_ids.extend(quote!{#temp,});
            }
        }
        
        let mut fields = quote!{};
        let mut ser = quote!{};
        let mut des = quote!{};

        for field in &data.fields{
            if let syn::Type::Path(ref ty) = field.ty{
                if let Some(seg) = ty.path.segments.first(){
                    if seg.ident.to_string().contains("Darc") && !local{
                        let (serialize,deserialize) = if seg.ident.to_string().contains("LocalRwDarc"){
                            let serialize = format!("{}::localrw_serialize",crate_header);
                            let deserialize = format!("{}::localrw_from_ndarc",crate_header);
                            (serialize,deserialize)
                        }
                        else{
                            let serialize = format!("{}::darc_serialize",crate_header);
                            let deserialize = format!("{}::darc_from_ndarc",crate_header);
                            (serialize,deserialize)
                        };
                        
                        fields.extend(quote_spanned!{field.span()=>
                            #[serde(serialize_with = #serialize, deserialize_with = #deserialize)]
                            #field,
                        });
                        let field_name = field.ident.clone();
                        ser.extend(quote_spanned!{field.span()=>
                            self.#field_name.serialize_update_cnts(num_pes);
                            // println!("serialized darc");
                            // self.#field_name.print();
                        });
                        des.extend(quote_spanned!{field.span()=>
                            self.#field_name.deserialize_update_cnts();
                            // println!("deserialized darc");
                            // self.#field_name.print();
                        });
                    }
                    else{
                        fields.extend(quote_spanned!{field.span()=>
                            #field,
                        });
                    }
                }
            }
        }
        
        // let traits = if args.to_string().len()>0{
        let mut impls = quote!{};
        if !local {
            impls.extend (quote!{ serde::Serialize, serde::Deserialize, });
        }
        else{
            ser.extend (quote! {panic!{"should not serialize data in LocalAM"}});
        }
        for t in args.to_string().split(","){
            if !t.contains("serde::Serialize") && !t.contains("serde::Deserialize") && t.trim().len()>0{
                let temp  = quote::format_ident!("{}",t.trim());
                impls.extend(quote!{#temp, });
            }

        }
        let traits = quote!{ #[derive( #impls)] };
        // println!("traits: {:#?}",traits);
        // }
        // else{
        //     quote!{ #[derive(serde::Serialize, serde::Deserialize)]}
        // };
        output.extend(quote!{  
            #traits           
            struct #name#generics{
                #fields
            }
            impl #generics#lamellar::DarcSerde for #name<#generics_ids>{
                fn ser (&self,  num_pes: usize) {
                    #ser
                } 
                fn des (&self){
                    #des
                }
            } 
        });

        
    }
    // println!("{:?}",input);
    TokenStream::from(output)
}

#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmData(args: TokenStream,input: TokenStream) -> TokenStream {
    derive_am_data(input,args,"lamellar".to_string(),false)
}

#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmLocalData(args: TokenStream,input: TokenStream) -> TokenStream {
    derive_am_data(input,args,"lamellar".to_string(),true)
}

#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmDataRT(args: TokenStream,input: TokenStream) -> TokenStream {
    derive_am_data(input, args,"crate".to_string(),false)
}

#[allow(non_snake_case)]
#[proc_macro_error]
#[proc_macro_attribute]
pub fn AmLocalDataRT(args: TokenStream,input: TokenStream) -> TokenStream {
    derive_am_data(input, args,"crate".to_string(),true)
}

fn parse_am(args: TokenStream, input: TokenStream, local: bool, rt: bool) -> TokenStream{
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
                    generate_am(input,local,rt, AmType::NoReturn)
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
    parse_am(args,input,false,false)
}

#[proc_macro_error]
#[proc_macro_attribute]
pub fn local_am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args,input,true,false)
}

#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args,input,false,true)
}

#[proc_macro_error]
#[proc_macro_attribute]
pub fn rt_am_local(args: TokenStream, input: TokenStream) -> TokenStream {
    parse_am(args,input,true,true)
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
            fn exec(self: Box<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<lamellar::LamellarReturn>> + Send>>{

                Box::pin( async move {
                #temp
                let ret = lamellar::serialize(& #last_expr ).unwrap();
                let ret = match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
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

        impl LocalAM for #orig_name {
            type Output = #output;
        }

        impl LamellarAM for #orig_name {
            type Output = #output;
        }

        fn #orig_name_exec(bytes: Vec<u8>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __lamellar_world: std::sync::Arc<lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<lamellar::LamellarTeamRT>,return_am) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<lamellar::LamellarReturn>> + Send>> {
            let __lamellar_data: Box<#orig_name> = Box::new(lamellar::deserialize(&bytes).unwrap());
            <#orig_name as lamellar::LamellarActiveMessage>::exec(__lamellar_data,__lamellar_current_pe,__lamellar_num_pes,return_am,__lamellar_world,__lamellar_team)
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
        impl #lamellar::LamellarSerde for #reduction_name {
            fn ser(&self,_num_pes: usize) -> Vec<u8> {
                #lamellar::serialize(self).unwrap()
            }
            fn des(&self) {} 
        }
        impl #lamellar::LamellarActiveMessage for #reduction_name {
            fn exec(self: Box<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeamRT>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeamRT>) -> std::pin::Pin<Box<dyn std::future::Future<Output=Option<#lamellar::LamellarReturn>> + Send>>{

                Box::pin( async move {
                let ret = #lamellar::serialize(& #exec ).unwrap();
                let ret = match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                    true => Some(#lamellar::LamellarReturn::LocalData(ret)),
                    false => Some(#lamellar::LamellarReturn::RemoteData(ret)),
                };
                ret
                })
            }
            fn get_id(&self) -> String{
                stringify!(#reduction_name).to_string()
            }
            
        }

        impl LocalAM for #reduction_name {
            type Output =  #typeident;
        }

        impl LamellarAM for #reduction_name {
            type Output = #typeident;
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

