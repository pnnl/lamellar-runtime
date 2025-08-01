use crate::replace::LamellarDSLReplace;
use crate::{get_expr, get_impl_method, type_name, AmType};

use proc_macro2::Span;
use quote::{quote, quote_spanned, ToTokens};
use syn::fold::Fold;
use syn::spanned::Spanned;

fn impl_lamellar_active_message_trait(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    am_body: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    // let trace_name = quote! {stringify!(#am_name)};
    quote! {
        impl #impl_generics #lamellar::active_messaging::LamellarActiveMessage for #am_name #ty_generics #where_clause {
            fn exec(self: std::sync::Arc<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeam>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeam>) -> std::pin::Pin<Box<dyn std::future::Future<Output=#lamellar::active_messaging::LamellarReturn> + Send >>{
                let __lamellar_thread_id = #lamellar::LAMELLAR_THREAD_ID.with(|id| *id);
                Box::pin( async move {
                    #am_body
                }
                // .instrument(#lamellar::tracing::trace_span!(#trace_name)))
            )
            }

            fn get_id(&self) -> &'static str{
                stringify!(#am_name)
            }
        }
    }
}

pub(crate) fn impl_lamellar_am_trait(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    ret_type: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        #[async_trait::async_trait]
        impl #impl_generics #lamellar::active_messaging::LamellarAM for #am_name #ty_generics #where_clause {
            type Output = #ret_type;
            async fn exec(self) -> Self::Output{
                panic!("this should never be called")
            }
        }
    }
}

fn impl_serde_trait(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl  #impl_generics #lamellar::active_messaging::Serde for #am_name #ty_generics #where_clause {}
    }
}

pub(crate) fn impl_lamellar_serde_trait(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #lamellar::active_messaging::LamellarSerde for #am_name #ty_generics #where_clause {
            fn serialized_size(&self)->usize{
                #lamellar::serialized_size(self,true)
            }
            fn serialize_into(&self,buf: &mut [u8]){
                #lamellar::serialize_into(buf,self,true).expect("can serialize and enough space in buf");
            }
            fn serialize(&self)->Vec<u8>{
                #lamellar::serialize(self,true).expect("can serialize")
            }
        }
    }
}

fn impl_return_lamellar_serde_trait(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #lamellar::active_messaging::LamellarSerde for #am_name #ty_generics #where_clause {
            fn serialized_size(&self)->usize{
                #lamellar::serialized_size(&self.val,true)
            }
            fn serialize_into(&self,buf: &mut [u8]){
                #lamellar::serialize_into(buf,&self.val,true).expect("can serialize and enough space in buf");
            }
            fn serialize(&self)->Vec<u8>{
                #lamellar::serialize(self,true).expect("can serialize")
            }
        }
    }
}

pub(crate) fn impl_lamellar_result_serde_trait(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    ret_type: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #lamellar::active_messaging::LamellarResultSerde for #am_name #ty_generics #where_clause {
            fn serialized_result_size(&self,result: & Box<dyn std::any::Any + Sync + Send>)->usize{
                let result  = result.downcast_ref::<#ret_type>().expect("can downcast result box");
                #lamellar::serialized_size(result,true)
            }
            fn serialize_result_into(&self,buf: &mut [u8],result: & Box<dyn std::any::Any + Sync + Send>){
                let result  = result.downcast_ref::<#ret_type>().expect("can downcast result box");
                #lamellar::serialize_into(buf,result,true).expect("can serialize and enough size in buf");
            }
        }
    }
}

pub(crate) fn impl_remote_active_message_trait(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #lamellar::active_messaging::RemoteActiveMessage for #am_name #ty_generics #where_clause {
        fn as_local(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn #lamellar::active_messaging::LamellarActiveMessage + Send + Sync>{
            self
        }
    }}
}

fn impl_lamellar_result_darc_serde_trait(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #lamellar::active_messaging::LamellarResultDarcSerde for #am_name #ty_generics #where_clause{}
    }
}

fn impl_unpack_and_register_function(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, _where_clause) = generics.split_for_impl();
    let am_name_unpack = quote::format_ident!("{}_unpack", am_name.clone());
    quote! {
        fn #am_name_unpack #impl_generics (bytes: &[u8], cur_pe: Result<usize,#lamellar::IdError>) -> std::sync::Arc<dyn #lamellar::active_messaging::RemoteActiveMessage + Sync + Send>  {
            let __lamellar_data: std::sync::Arc<#am_name #ty_generics> = std::sync::Arc::new(#lamellar::deserialize(&bytes,true).expect("can deserialize into remote active message"));
            <#am_name #ty_generics as #lamellar::active_messaging::DarcSerde>::des(&__lamellar_data,cur_pe);
            __lamellar_data
        }

        #lamellar::inventory::submit! {
            #lamellar::active_messaging::RegisteredAm{
                exec: #am_name_unpack,
                name: stringify!(#am_name)
            }
        }
    }
}

pub(crate) fn impl_local_am_trait(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    ret_type: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #lamellar::active_messaging::LocalAM for #am_name #ty_generics #where_clause{
            type Output = #ret_type;
        }
    }
}

fn impl_darc_serde_trait(
    generics: &syn::Generics,
    name: &syn::Ident,
    ser: &proc_macro2::TokenStream,
    des: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #lamellar::active_messaging::DarcSerde for #name #ty_generics #where_clause{
            fn ser (&self,  num_pes: usize, darcs: &mut Vec<#lamellar::active_messaging::RemotePtr>){
                #ser
            }
            fn des (&self,cur_pe: Result<usize, #lamellar::IdError>){
                #des
            }
        }
    }
}

pub(crate) fn impl_remote_traits(
    generics: &syn::Generics,
    am_name: &syn::Ident,
    ret_type: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> proc_macro2::TokenStream {
    if local {
        quote! {}
    } else {
        let lamellaram = impl_lamellar_am_trait(generics, am_name, ret_type, lamellar);
        let serde = impl_serde_trait(generics, am_name, lamellar);
        let lamellar_serde = impl_lamellar_serde_trait(generics, am_name, lamellar);
        let lamellar_result_serde =
            impl_lamellar_result_serde_trait(generics, am_name, ret_type, lamellar);
        let impl_remote_active_message =
            impl_remote_active_message_trait(generics, am_name, lamellar);
        let unpack_reg_fn = impl_unpack_and_register_function(generics, am_name, lamellar);

        quote! {
            #lamellaram
            #serde
            #lamellar_serde
            #lamellar_result_serde
            #impl_remote_active_message
            #unpack_reg_fn
        }
    }
}

fn gen_am_body(
    input: &syn::ItemImpl,
    am_type: &AmType,
    ret_struct_name: &syn::Ident,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> (proc_macro2::TokenStream, bool) {
    let mut exec_fn =
        get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
    // let func_span = exec_fn.span();
    // exec_fn = replace_lamellar_dsl_new(exec_fn);
    exec_fn = LamellarDSLReplace.fold_block(exec_fn);

    let mut am_body = quote_spanned! {exec_fn.span()=>};
    let mut stmts = exec_fn.stmts;

    let (ret_statement, bytes_buf) = if let Some(stmt) = stmts.pop() {
        gen_return_stmt(am_type, &stmt, &ret_struct_name, &lamellar, local)
    } else {
        (
            quote! {#lamellar::active_messaging::LamellarReturn::Unit},
            false,
        )
    };

    for stmt in stmts {
        am_body.extend(quote_spanned! {stmt.span()=>
            #stmt
        });
    }
    am_body.extend(ret_statement);
    (am_body, bytes_buf)
}

fn gen_return_stmt(
    am_type: &AmType,
    last_stmt: &syn::Stmt,
    ret_struct_name: &syn::Ident,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> (proc_macro2::TokenStream, bool) {
    let mut byte_buf = false;
    let ret_stmt = match am_type {
        AmType::NoReturn => quote_spanned! {last_stmt.span()=>
            #last_stmt
            #lamellar::active_messaging::LamellarReturn::Unit
        },
        AmType::ReturnData(ref ret) => {
            let last_expr = get_expr(&last_stmt)
                .expect("failed to get exec return value (try removing the last \";\")");
            let last_expr =
                quote_spanned! {last_stmt.span()=> let __lamellar_last_expr: #ret = #last_expr; };
            let remote_last_expr = match ret {
                syn::Type::Array(a) => match &*a.elem {
                    syn::Type::Path(type_path)
                        if type_path.clone().into_token_stream().to_string() == "u8" =>
                    {
                        byte_buf = true;
                        quote_spanned! {last_stmt.span()=> ByteBuf::from(__lamellar_last_expr)}
                    }
                    _ => quote_spanned! {last_stmt.span()=> __lamellar_last_expr},
                },
                _ => quote_spanned! {last_stmt.span()=> __lamellar_last_expr},
            };
            if !local {
                quote_spanned! {last_stmt.span()=>
                    #last_expr
                    let ret = match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                        true => #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(__lamellar_last_expr)),
                        false => #lamellar::active_messaging::LamellarReturn::RemoteData(std::sync::Arc::new (#ret_struct_name{
                            val: #remote_last_expr,
                        })),
                    };
                    ret
                }
            } else {
                quote_spanned! {last_stmt.span()=>
                    #last_expr
                    #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(__lamellar_last_expr))
                }
            }
        }
        AmType::ReturnAm(ret, _) => {
            let last_expr = get_expr(&last_stmt)
                .expect("failed to get exec return value (try removing the last \";\")");
            let last_expr =
                quote_spanned! {last_stmt.span()=> let __lamellar_last_expr: #ret = #last_expr; };
            if !local {
                quote_spanned! {last_stmt.span()=>
                    #last_expr
                    let ret = match __local{
                        true => #lamellar::active_messaging::LamellarReturn::LocalAm(std::sync::Arc::new(__lamellar_last_expr)),
                        false => #lamellar::active_messaging::LamellarReturn::RemoteAm(std::sync::Arc::new(__lamellar_last_expr)),
                    };
                    ret
                }
            } else {
                quote_spanned! {last_stmt.span()=>
                    #last_expr
                    #lamellar::active_messaging::LamellarReturn::LocalAm(std::sync::Arc::new(__lamellar_last_expr))
                }
            }
        }
    };
    (ret_stmt, byte_buf)
}

pub(crate) fn impl_return_struct(
    generics: &syn::Generics,
    am_data_header: &proc_macro2::TokenStream,
    ret_struct_name: &syn::Ident,
    ret_type: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
    bytes_buf: bool,
    local: bool,
) -> proc_macro2::TokenStream {
    let (impl_generics, _ty_generics, where_clause) = generics.split_for_impl();

    let generic_phantoms = generics.type_params().fold(quote! {}, |acc, t| {
        let name = quote::format_ident!("_phantom_{}", t.ident.to_string().to_lowercase());
        let t = &t.ident;
        quote! {#acc
        #name: std::marker::PhantomData<#t>,}
    });

    let mut the_ret_struct = if bytes_buf {
        quote! {
            #am_data_header
            struct #ret_struct_name #impl_generics #where_clause{
                val: serde_bytes::ByteBuf,
                #generic_phantoms
            }
        }
    } else {
        quote! {
            #am_data_header
            struct #ret_struct_name #impl_generics #where_clause{
                val: #ret_type,
                #generic_phantoms
            }
        }
    };

    if !local {
        the_ret_struct.extend(impl_return_lamellar_serde_trait(
            generics,
            ret_struct_name,
            lamellar,
        ));
        the_ret_struct.extend(impl_lamellar_result_darc_serde_trait(
            generics,
            ret_struct_name,
            lamellar,
        ));
    }
    the_ret_struct
}

pub(crate) fn generate_am(
    input: &syn::ItemImpl,
    local: bool,
    am_type: AmType,
    lamellar: &proc_macro2::TokenStream,
    am_data_header: &proc_macro2::TokenStream,
) -> proc_macro::TokenStream {
    let name = type_name(&input.self_ty).expect("unable to find name");
    let orig_name = syn::Ident::new(&name, Span::call_site());
    let return_struct_name = quote::format_ident!("{}Result", orig_name);

    let generics = input.generics.clone();

    let (am_body, bytes_buf) = gen_am_body(&input, &am_type, &return_struct_name, &lamellar, local);

    let (return_type, return_struct) = {
        match am_type {
            AmType::NoReturn => (quote! {()}, quote! {}),
            AmType::ReturnData(ref output) => {
                let return_type = quote! {#output};
                let return_struct = impl_return_struct(
                    &generics,
                    &am_data_header,
                    &return_struct_name,
                    &return_type,
                    &lamellar,
                    bytes_buf,
                    local,
                );
                (return_type, return_struct)
            }
            AmType::ReturnAm(ref _am, ref output) => (quote! {#output}, quote! {}),
        }
    };

    let lamellar_active_message =
        impl_lamellar_active_message_trait(&generics, &orig_name, &am_body, &lamellar);
    let local_am = impl_local_am_trait(&generics, &orig_name, &return_type, &lamellar);
    let remote_trait_impls =
        impl_remote_traits(&generics, &orig_name, &return_type, &lamellar, local);

    let expanded = quote_spanned! {am_body.span()=>
        #lamellar_active_message

        #local_am

        #remote_trait_impls

        #return_struct
    };

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            // use __lamellar::tracing::*;
            // use __lamellar::Instrument;
            #expanded
        };
    };

    let rt_expanded = quote_spanned! {
        expanded.span()=>
        const _: () = {
            // //use tracing::*;
            #expanded
        };
    };

    if lamellar.to_string() == "crate" {
        proc_macro::TokenStream::from(rt_expanded)
    } else {
        proc_macro::TokenStream::from(user_expanded)
    }
}

pub(crate) fn create_am_struct(
    generics: &syn::Generics,
    attributes: &proc_macro2::TokenStream,
    traits: &proc_macro2::TokenStream,
    attrs: &proc_macro2::TokenStream,
    vis: &proc_macro2::TokenStream,
    name: &syn::Ident,
    fields: &proc_macro2::TokenStream,
    ser: &proc_macro2::TokenStream,
    des: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
    _local: bool,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let (impl_generics, _ty_generics, where_clause) = generics.split_for_impl();

    let the_struct = quote! {
        #attributes
        #traits
        #attrs
        #vis struct #name #impl_generics #where_clause{
            #fields
        }
    };

    let darc_serde = impl_darc_serde_trait(generics, name, ser, des, lamellar);

    (the_struct, darc_serde)
}
