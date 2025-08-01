use crate::field_info::FieldInfo;
use crate::gen_am::*;
use crate::replace::{LamellarDSLReplace, ReplaceSelf};
use crate::{get_impl_method, type_name, AmType};

use proc_macro2::Span;
use quote::{format_ident, quote, quote_spanned, ToTokens};
use syn::fold::Fold;
use syn::spanned::Spanned;

fn impl_am_group_remote_lamellar_active_message_trait(
    generics: &syn::Generics,
    am_group_am_name: &syn::Ident,
    am_group_body: &proc_macro2::TokenStream,
    ret_contatiner: &proc_macro2::TokenStream,
    ret_push: &proc_macro2::TokenStream,
    ret_stmt: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    // let trace_name = quote! {stringify!(#am_group_am_name)};
    // println!("ret_contatiner: {}", ret_contatiner.to_string());
    // println!("ret_push: {}", ret_push.to_string());
    // println!("ret_stmt: {}", ret_stmt.to_string());
    quote! {
        impl #impl_generics #lamellar::active_messaging::LamellarActiveMessage for #am_group_am_name #ty_generics #where_clause {
            fn exec(self: std::sync::Arc<Self>,__lamellar_current_pe: usize,__lamellar_num_pes: usize, __local: bool, __lamellar_world: std::sync::Arc<#lamellar::LamellarTeam>, __lamellar_team: std::sync::Arc<#lamellar::LamellarTeam>) -> std::pin::Pin<Box<dyn std::future::Future<Output=#lamellar::active_messaging::LamellarReturn> + Send >>{
                let __lamellar_thread_id = #lamellar::LAMELLAR_THREAD_ID.with(|id| *id);
                Box::pin( async move {
                    #ret_contatiner
                    for i in 0..self.len(){
                        let _e = { #am_group_body };
                        #ret_push
                    }
                    #ret_stmt
                    }
                    // }.instrument(#lamellar::tracing::trace_span!(#trace_name))
                )
            }
            fn get_id(&self) -> &'static str{
                stringify!(#am_group_am_name)//.to_string()
            }
        }
    }
}

fn gen_am_group_remote_body2(
    am_name: &syn::Ident,
    input: &syn::ItemImpl,
) -> proc_macro2::TokenStream {
    let mut exec_fn =
        get_impl_method("exec".to_string(), &input.items).expect("unable to extract exec body");
    // exec_fn = replace_lamellar_dsl_new(exec_fn);
    exec_fn = LamellarDSLReplace.fold_block(exec_fn);

    // exec_fn = replace_self(exec_fn, "am"); // we wont change self to am if its referencing a static_var
    let mut replace_self = ReplaceSelf::new(am_name.clone());
    exec_fn = replace_self.fold_block(exec_fn);

    // println!("{}", exec_fn.to_token_stream().to_string());
    let accessed_fields = replace_self.accessed_fields;

    let mut am_body = quote_spanned! { exec_fn.span()=>  };

    for (_, field) in accessed_fields {
        am_body.extend(quote! {#field});
    }
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
) -> (
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
) {
    match am_type {
        AmType::NoReturn => {
            //quote!{#lamellar::active_messaging::LamellarReturn::Unit},
            if !local {
                (
                    quote! {},
                    quote! {},
                    quote! {
                        match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                            true => #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(())),
                            false => #lamellar::active_messaging::LamellarReturn::RemoteData(std::sync::Arc::new (#am_group_return_name{
                                val: (),
                            })),
                        }
                    },
                )
            } else {
                (
                    quote! {},
                    quote! {},
                    quote! {#lamellar::active_messaging::LamellarReturn::LocalData(Box::new(())),},
                )
            }
        }
        AmType::ReturnData(ref _ret) => {
            if !local {
                (
                    quote! {let mut __res_vec = Vec::new();},
                    quote! {
                        __res_vec.push(_e);
                    },
                    quote! {
                        match __local{ //should probably just separate these into exec_local exec_remote to get rid of a conditional...
                            true => #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(__res_vec)),
                            false => #lamellar::active_messaging::LamellarReturn::RemoteData(std::sync::Arc::new (#am_group_return_name{
                                val: __res_vec,
                            })),
                        }
                    },
                )
            } else {
                (
                    quote! {let mut __res_vec = Vec::new();},
                    quote! {
                        __res_vec.push(_e);
                    },
                    quote! {
                        #lamellar::active_messaging::LamellarReturn::LocalData(Box::new(__res_vec))
                    },
                )
            }
        }
        AmType::ReturnAm(ref am, ref _output) => {
            let return_am_name: syn::Ident = format_ident!("{}", am.to_token_stream().to_string());
            let return_am_group_name = get_am_group_name(&return_am_name);
            if !local {
                (
                    quote! {let mut __am_group: Option<#return_am_group_name> = None;},
                    quote! {
                        match __am_group{
                           Some( ref mut __amg) => __amg.add_am(_e),
                           None => {
                                let mut __amg = #return_am_group_name::new(&_e);
                                __amg.add_am(_e);
                                let mut new_am_group = Some(__amg);
                                std::mem::swap(&mut __am_group, &mut new_am_group);

                            }
                        }
                    },
                    quote! {
                            match __local{
                            true => #lamellar::active_messaging::LamellarReturn::LocalAm(std::sync::Arc::new (__am_group.expect("am group should exsit"))),
                            false => #lamellar::active_messaging::LamellarReturn::RemoteAm(std::sync::Arc::new (__am_group.expect("am group should exsit"))),
                        }
                    },
                )
            } else {
                (
                    quote! {let mut __am_group: Option<#return_am_group_name> = None;},
                    quote! {
                        match __am_group{
                           Some( ref mut __amg) => __amg.add_am(_e),
                           None => {
                                let mut __amg = #return_am_group_name::new(&_e);
                                __amg.add_am(_e);
                                let mut new_am_group = Some(__amg);
                                std::mem::swap(&mut __am_group, &mut new_am_group);
                            }
                        }
                    },
                    quote! {
                        #lamellar::active_messaging::LamellarReturn::LocalAm(std::sync::Arc::new (__am_group.expect("am group should exsit")))
                    },
                )
            }
        }
    }
}

fn impl_am_group_remote(
    generics: &syn::Generics,
    am_group_am_name: &syn::Ident,
    am_group_body: &proc_macro2::TokenStream,
    ret_type: &proc_macro2::TokenStream,
    ret_contatiner: &proc_macro2::TokenStream,
    ret_push: &proc_macro2::TokenStream,
    ret_stmt: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let lamellar_active_message = impl_am_group_remote_lamellar_active_message_trait(
        generics,
        am_group_am_name,
        am_group_body,
        ret_contatiner,
        ret_push,
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

fn get_am_group_name(name: &syn::Ident) -> syn::Ident {
    syn::Ident::new(&format!("{}GroupAm", name), name.span())
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
    am_group_remote_name: &syn::Ident,
    am_group_name_user: &syn::Ident,
    _inner_ret_type: &proc_macro2::TokenStream,
    lamellar: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let mut am_user_generics = generics.clone();
    am_user_generics.params.push(syn::GenericParam::Type(
        syn::parse_str(&format!("Am: {}::active_messaging::LamellarAM", lamellar)).unwrap(),
    ));
    let mut typed_am_group_result_return_type = format!("{}::TypedAmGroupResult<", lamellar);
    am_user_generics.type_params_mut().for_each(|tp| {
        if tp.ident == "Am" {
            typed_am_group_result_return_type += "Am::Output,";
        } else {
            typed_am_group_result_return_type += &format!("{},", tp.ident);
        }
    });
    typed_am_group_result_return_type += ">";
    let typed_am_group_result_return_type: syn::Type =
        syn::parse_str(&typed_am_group_result_return_type).unwrap();
    let (am_user_impl_generics, am_user_ty_generics, am_user_where_clause) =
        am_user_generics.split_for_impl();
    let (_impl_generics, ty_generics, _where_clause) = generics.split_for_impl();

    let (typed_am_group_result_type, add_all_req_type, single_req_type) = match am_type {
        AmType::NoReturn => (
            quote! {
                #lamellar::TypedAmGroupResult::unit(
                    results,
                    self.cnt,
                    num_pes
                )
            },
            quote! {#lamellar::BaseAmGroupReq::AllPeUnit},
            quote! {#lamellar::BaseAmGroupReq::SinglePeUnit},
        ),
        AmType::ReturnData(ref _output) => (
            quote! {
                #lamellar::TypedAmGroupResult::val(
                    results,
                    self.cnt,
                    num_pes
                )
            },
            quote! {#lamellar::BaseAmGroupReq::AllPeVal},
            quote! {#lamellar::BaseAmGroupReq::SinglePeVal},
        ),
        AmType::ReturnAm(ref _am, ref output) => {
            if output.to_string() == "()" {
                (
                    quote! {
                        #lamellar::TypedAmGroupResult::unit(
                            results,
                            self.cnt,
                            num_pes
                        )
                    },
                    quote! {#lamellar::BaseAmGroupReq::AllPeUnit},
                    quote! {#lamellar::BaseAmGroupReq::SinglePeUnit},
                )
            } else {
                (
                    quote! {
                        #lamellar::TypedAmGroupResult::val(
                            results,
                            self.cnt,
                            num_pes
                        )
                    },
                    quote! {#lamellar::BaseAmGroupReq::AllPeVal},
                    quote! {#lamellar::BaseAmGroupReq::SinglePeVal},
                )
            }
        }
    };

    // quote! {
    //     //#[doc(hidden)]
    //     pub struct #am_group_name_user #impl_generics #where_clause{
    //         team: std::sync::Arc<#lamellar::LamellarTeam>,
    //         batch_cnt: usize,
    //         cnt: usize,
    //         reqs: std::collections::BTreeMap<usize,(Vec<usize>, #am_group_remote_name #ty_generics,usize)>,
    //         num_per_batch: usize,
    //         pending_reqs: Vec<#lamellar::TypedAmGroupBatchReq<#inner_ret_type>>,
    //     }
    quote! {
        impl #am_user_impl_generics #am_group_name_user #am_user_ty_generics #am_user_where_clause{
            pub fn new(team: std::sync::Arc<#lamellar::LamellarTeam>) -> Self {
                let num_per_batch = #lamellar::config().am_group_batch_size;
                // match std::env::var("LAMELLAR_OP_BATCH") {
                //     Ok(n) => n.parse::<usize>().unwrap(),
                //     Err(_) => 10000,
                // };
                #am_group_name_user {
                    team: team,
                    batch_cnt: 0,
                    cnt: 0,
                    reqs: std::collections::BTreeMap::new(),
                    num_per_batch: num_per_batch,
                    pending_reqs: Vec::new(),
                }
            }

            pub fn add_am_all(&mut self, am:  #am_name #ty_generics)
            {
                self.add_am_pe(self.team.num_pes(),am);
            }

            pub fn add_am_pe(&mut self, pe: usize, am:  #am_name #ty_generics)
            {

                let req_queue = self.reqs.entry(pe).or_insert_with(|| {
                    (Vec::with_capacity(self.num_per_batch),#am_group_remote_name::new(&am),0)
                });
                req_queue.0.push(self.cnt);
                self.cnt += 1;
                self.batch_cnt += 1;
                req_queue.1.add_am(am);

                if self.batch_cnt >= self.num_per_batch  {
                    self.send_pe_buffer(pe);
                    self.batch_cnt = 0;
                }
            }

            fn send_pe_buffer(&mut self, pe: usize) {
                if let Some((reqs,the_am,cnt)) = self.reqs.remove(&pe){
                    if pe == self.team.num_pes(){
                        self.pending_reqs.push(#lamellar::TypedAmGroupBatchReq::new(pe,reqs,#add_all_req_type(self.team.exec_am_group_all(the_am))));
                    }
                    else{
                        self.pending_reqs.push(#lamellar::TypedAmGroupBatchReq::new(pe,reqs,#single_req_type(self.team.exec_am_group_pe(pe,the_am))));
                    }
                }
            }

            // #[#lamellar::instrument(skip_all, level = "debug")]
            pub async fn exec(mut self) -> #typed_am_group_result_return_type{

                // #lamellar::trace!("typed_am_group exec");
                // let timer = std::time::Instant::now();

                for pe in 0..(self.team.num_pes()+1){
                    self.send_pe_buffer(pe);
                }

                // #lamellar::trace!("{} pending reqs", self.pending_reqs.len());
                let results = #lamellar::futures_util::future::join_all(self.pending_reqs.drain(..).map(|req| async { let req = req.into_result().await;
                    // #lamellar::trace!("got result");
                    req
                })).await;
                let num_pes = self.team.num_pes();
                #typed_am_group_result_type
            }
        }
    }
}

fn generate_am_group_user_struct(
    generics: &syn::Generics,
    vis: &proc_macro2::TokenStream,
    am_group_name_user: &syn::Ident,
    am_group_remote_name: &syn::Ident,
) -> proc_macro2::TokenStream {
    let mut am_user_generics = generics.clone();
    am_user_generics.params.push(syn::GenericParam::Type(
        syn::parse_str("Am: lamellar::active_messaging::LamellarAM").unwrap(),
    ));
    let (am_user_impl_generics, _am_user_ty_generics, am_user_where_clause) =
        am_user_generics.split_for_impl();

    let (_impl_generics, ty_generics, _where_clause) = generics.split_for_impl();

    quote! {
        //#[doc(hidden)]
        #vis struct #am_group_name_user #am_user_impl_generics #am_user_where_clause{
            team: std::sync::Arc<lamellar::LamellarTeam>,
            batch_cnt: usize,
            cnt: usize,
            reqs: std::collections::BTreeMap<usize,(Vec<usize>, #am_group_remote_name #ty_generics,usize)>,
            num_per_batch: usize,
            pending_reqs: Vec<lamellar::TypedAmGroupBatchReq<Am::Output>>,
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
    let am_group_am_name = get_am_group_name(&format_ident! {"{}", &orig_name});
    let am_group_user_name = get_am_group_user_name(&orig_name);
    let am_group_return_name = get_am_group_return_name(&orig_name);

    let generics = input.generics.clone();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let (ret_type, inner_ret_type) = {
        match am_type {
            AmType::NoReturn => (quote! {()}, quote! {()}),
            AmType::ReturnData(ref output) => (quote! {Vec<#output>}, quote! {#output}),
            AmType::ReturnAm(ref _am, ref output) => {
                if output.to_string() == "()" {
                    (quote! {()}, quote! {()})
                } else {
                    (quote! {Vec<#output>}, quote! {#output})
                }
            }
        }
    };

    let am_group_remote_body = gen_am_group_remote_body2(&orig_name, &input);

    let (ret_contatiner, ret_push, ret_stmt) =
        gen_am_group_return_stmt(&am_type, &am_group_return_name, lamellar, false);

    let am_group_remote = impl_am_group_remote(
        &generics,
        &am_group_am_name,
        &am_group_remote_body,
        &ret_type,
        &ret_contatiner,
        &ret_push,
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
        &am_group_am_name,
        &am_group_user_name,
        &inner_ret_type,
        &lamellar,
    );

    let mut am_user_generics = generics.clone();
    am_user_generics.params.push(syn::GenericParam::Type(
        syn::parse_str(&format!("Am: {}::active_messaging::LamellarAM", lamellar)).unwrap(),
    ));
    let mut am_group_user_type = format!("{}<", am_group_user_name);
    am_user_generics.type_params_mut().for_each(|tp| {
        if tp.ident == "Am" {
            am_group_user_type += "Self,";
        } else {
            am_group_user_type += &format!("{},", tp.ident);
        }
    });
    am_group_user_type += ">";
    let am_group_user_type: syn::Type = syn::parse_str(&am_group_user_type).unwrap();
    // let (_am_user_impl_generics, _am_user_ty_generics, _am_user_where_clause) =
    //     am_user_generics.split_for_impl();

    let mut create_am_user_generics = generics.clone();
    create_am_user_generics.params.push(syn::GenericParam::Type(
        syn::parse_str(&format!("U: Into<{}::ArcLamellarTeam>", lamellar)).unwrap(),
    ));
    let (create_am_user_impl_generics, _create_am_user_ty_generics, _create_am_user_where_clause) =
        create_am_user_generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics #orig_name #ty_generics #where_clause{
            pub fn create_am_group #create_am_user_impl_generics (team: U) -> #am_group_user_type{
                #am_group_user_name::new(team.into().team.clone())
            }
        }

        #am_group_remote

        #am_group_return

        #am_group_user
    };

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
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
    let am_group_name = get_am_group_name(&format_ident!("{}", name));
    let fields_as_vecs = fields.to_tokens_as_vecs();
    let am_group_fields = quote! {
        #static_fields
        #fields_as_vecs
    };
    let mut am_group_ser = fields.ser_as_vecs();
    am_group_ser.extend(static_fields.ser());

    let mut am_group_des = fields.des_as_vecs();
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

    let field_getters = fields.gen_getters(false);
    let static_field_getters = static_fields.gen_getters(true);

    let field_inits = fields.names().iter().fold(quote! {}, |acc, n| {
        quote! {
            #acc
            self.#n.push(am.#n);
        }
    });
    let field_news = fields.names().iter().fold(quote! {}, |acc, n| {
        quote! {
            #acc
            #n: vec![],
        }
    });

    let my_len = if fields.names().len() > 0 {
        let f = &fields.names()[0];
        quote! {
            self.#f.len()
        }
    } else {
        quote! {
            1
        }
    };
    let static_field_inits = static_fields.names().iter().fold(quote! {}, |acc, n| {
        quote! {
            #acc
            #n: am.#n.clone(),
        }
    });

    (
        the_struct,
        quote! {
            impl #impl_generics #am_group_name #ty_generics #where_clause{

                #field_getters
                #static_field_getters

                 fn new(am: &#name) -> Self {
                    #am_group_name {
                        #field_news
                        #static_field_inits
                    }
                }

                 fn add_am(&mut self, am: #name) {
                    #field_inits
                }

                 fn len(&self) -> usize {
                    #my_len
                }
            }

            #the_traits
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
    let am_group_user_struct = generate_am_group_user_struct(
        generics,
        vis,
        &get_am_group_user_name(&name),
        &get_am_group_name(&format_ident!("{}", name)),
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

    (
        quote! {
            #am_group_user_struct
            #am_group_remote
        },
        quote! {
            #am_group_remote_traits
        },
    )
}
