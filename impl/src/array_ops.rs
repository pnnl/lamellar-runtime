#![allow(clippy::type_complexity)]

use proc_macro::TokenStream;
// use proc_macro_error::abort;
use proc_macro2::Ident;
use quote::{quote, quote_spanned};
use syn::parse_macro_input;
use syn::spanned::Spanned;

fn type_to_string(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(path) => path
            .path
            .segments
            .iter()
            .fold(String::new(), |acc, segment| {
                acc + "_"
                    + &segment.ident.to_string()
                    + &match &segment.arguments {
                        syn::PathArguments::None => String::new(),
                        syn::PathArguments::AngleBracketed(args) => {
                            args.args.iter().fold(String::new(), |acc, arg| {
                                acc + "_"
                                    + &match arg {
                                        syn::GenericArgument::Type(ty) => type_to_string(ty),
                                        _ => panic!("unexpected argument type"),
                                    }
                            })
                        }
                        syn::PathArguments::Parenthesized(args) => args
                            .inputs
                            .iter()
                            .fold(String::new(), |acc, arg| acc + "_" + &type_to_string(arg)),
                    }
            }),
        _ => {
            panic!("unexpected type");
        }
    }
}

fn gen_multi_val_multi_idx(
    op_type: proc_macro2::TokenStream,
    lock: &proc_macro2::TokenStream,
    op: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    quote! {
        #op_type =>{
            for elem in idx_vals{
                let index = elem.index as usize;
                let val = elem.val;
                #lock
                #op
            }
        }
    }
}

fn gen_single_val_multi_idx(
    op_type: proc_macro2::TokenStream,
    lock: &proc_macro2::TokenStream,
    op: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    quote! {
        #op_type =>{
            for index in indices.iter(){
                let index = (*index) as usize;
                #lock
                #op
            }
        }
    }
}

fn gen_multi_val_single_idx(
    op_type: proc_macro2::TokenStream,
    lock: &proc_macro2::TokenStream,
    op: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    quote! {
        #op_type =>{
            #lock
            for val in vals.iter(){
                let val = *val;
                #op
            }
        }
    }
}

fn gen_array_names(
    array_type: &Ident,
    typeident: &syn::Type,
    val_type: &str,
    idx_type: &str,
) -> (Ident, Ident, Ident, Ident, Ident, Ident, Ident, Ident) {
    let base = quote::format_ident!(
        "{array_type}_{}_{val_type}_val_{idx_type}_idx",
        type_to_string(typeident)
    );

    let am_buf_name = quote::format_ident!("{base}_am_buf");
    let dist_am_buf_name = quote::format_ident!("{base}_am");
    let am_buf_fetch_name = quote::format_ident!("{base}_am_buf_fetch");
    let dist_am_buf_fetch_name = quote::format_ident!("{base}_am_fetch");
    let am_buf_result_name = quote::format_ident!("{base}_am_buf_result");
    let dist_am_buf_result_name = quote::format_ident!("{base}_am_result");
    let reg_name = quote::format_ident!("{val_type}_val_{idx_type}_idx_ops");
    let id_gen_name = quote::format_ident!("{base}_id");
    (
        am_buf_name,
        dist_am_buf_name,
        am_buf_fetch_name,
        dist_am_buf_fetch_name,
        am_buf_result_name,
        dist_am_buf_result_name,
        reg_name,
        id_gen_name,
    )
}
fn create_buf_ops(
    typeident: syn::Type,
    array_type: syn::Ident,
    byte_array_type: syn::Ident,
    optypes: &Vec<OpType>,
    rt: bool,
) -> proc_macro2::TokenStream {
    // println!("[lamellar_impl] creating buf ops for type: {:?} {:?} {:?}", typeident,array_type,optypes);
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
            syn::parse("__lamellar::AmData".parse().unwrap()).unwrap(),
            syn::parse("__lamellar::am".parse().unwrap()).unwrap(),
        )
    };

    let mut expanded = quote! {};
    let (
        lhs,
        assign,
        fetch_add,
        fetch_sub,
        fetch_mul,
        fetch_div,
        fetch_rem,
        fetch_and,
        fetch_or,
        fetch_xor,
        load,
        swap,
        compare_exchange,
        compare_exchange_eps,
        shl,
        fetch_shl,
        shr,
        fetch_shr,
    ) = if array_type == "NativeAtomicArray" {
        panic!("native atomics should be handled differently, this should never happen")
    } else if array_type == "ReadOnlyArray" {
        (
            quote! { panic!("assign a valid op for Read Only Arrays");}, //lhs
            quote! { panic!("assign/store not a valid op for Read Only Arrays");}, //assign
            quote! { panic!("fetch_add not a valid op for Read Only Arrays"); }, //fetch_add -- we lock the index before this point so its actually atomic
            quote! { panic!("fetch_sub not a valid op for Read Only Arrays"); }, //fetch_sub --we lock the index before this point so its actually atomic
            quote! { panic!("fetch_mul not a valid op for Read Only Arrays"); }, //fetch_mul --we lock the index before this point so its actually atomic
            quote! { panic!("fetch_div not a valid op for Read Only Arrays"); }, //fetch_div --we lock the index before this point so its actually atomic
            quote! { panic!("fetch_rem not a valid op for Read Only Arrays"); }, //fetch_rem --we lock the index before this point so its actually atomic
            quote! { panic!("fetch_and not a valid op for Read Only Arrays"); }, //fetch_and --we lock the index before this point so its actually atomic
            quote! { panic!("fetch_or not a valid op for Read Only Arrays"); }, //fetch_or --we lock the index before this point so its actually atomic
            quote! { panic!("fetch_xor not a valid op for Read Only Arrays"); }, //fetch_xor --we lock the index before this point so its actually atomic
            quote! {slice[index]},                                               //load
            quote! { panic!("swap not a valid op for Read Only Arrays"); }, //swap we lock the index before this point so its actually atomic
            quote! { panic!("compare exchange not a valid op for Read Only Arrays"); }, // compare_exchange -- we lock the index before this point so its actually atomic
            quote! { panic!("compare exchange eps not a valid op for Read Only Arrays"); }, //compare exchange epsilon
            quote! { panic!("shl not a valid op for Read Only Arrays"); },                  //shl
            quote! { panic!("fetch_shl not a valid op for Read Only Arrays"); }, //fetch_shl
            quote! { panic!("shr not a valid op for Read Only Arrays"); },       //shr
            quote! { panic!("fetch_shr not a valid op for Read Only Arrays"); }, //fetch_shr
        )
    } else {
        (
            quote! {slice[index]},       //lhs
            quote! {slice[index] = val}, //assign
            quote! {
                res.push(slice[index]); slice[index] += val;
            }, //fetch_add -- we lock the index before this point so its actually atomic
            quote! {
                res.push(slice[index]); slice[index] -= val;
            }, //fetch_sub --we lock the index before this point so its actually atomic
            quote! {
                res.push(slice[index]); slice[index] *= val;
            }, //fetch_mul --we lock the index before this point so its actually atomic
            quote! {
                res.push(slice[index]); slice[index] /= val;
            }, //fetch_div --we lock the index before this point so its actually atomic
            quote! {
                res.push(slice[index]); slice[index] %= val;
            }, //fetch_rem --we lock the index before this point so its actually atomic
            quote! {
                res.push(slice[index]); slice[index] &= val;
            }, //fetch_and --we lock the index before this point so its actually atomic
            quote! {
                res.push(slice[index]); slice[index] |= val;
            }, //fetch_or --we lock the index before this point so its actually atomic
            quote! {
                res.push(slice[index]); slice[index] ^= val;
            }, //fetch_xor --we lock the index before this point so its actually atomic
            quote! {slice[index]},       //load
            quote! {
                res.push(slice[index]); slice[index] = val;
            }, //swap we lock the index before this point so its actually atomic
            quote! {  // compare_exchange -- we lock the index before this point so its actually atomic
                 let t_res = if old == slice[index]{
                    slice[index] = val;
                    Ok(old)
                } else {
                    Err(slice[index])
                };
                res.push(t_res);
            },
            quote! { //compare exchange epsilon
                let same = if old > slice[index] {
                    old - slice[index] < eps
                }
                else{
                    slice[index] - old < eps
                };
                let t_res = if same {
                    slice[index] = val;
                    Ok(old)
                } else {
                    Err(slice[index])
                };
                res.push(t_res);
            },
            quote! { slice[index] <<= val; }, //shl --we lock the index before this point so its actually atomic
            quote! {
                res.push(slice[index]); slice[index] <<= val;
            }, //fetch_shl --we lock the index before this point so its actually atomic
            quote! { slice[index] >>= val; }, //shr --we lock the index before this point so its actually atomic
            quote! {
                res.push(slice[index]); slice[index] >>= val;
            }, //fetch_shr --we lock the index before this point so its actually atomic
        )
    };
    let (lock, slice) = if array_type == "GenericAtomicArray" {
        (
            quote! {let _lock = self.data.lock_index(index);},
            quote! {let mut slice = unsafe{self.data.__local_as_mut_slice()};},
        )
    } else if array_type == "NativeAtomicArray" {
        panic!("native atomics should be handled differently, this should never happen")
    } else if array_type == "LocalLockArray" || array_type == "GlobalLockArray" {
        (
            quote! {}, //no explicit lock since the slice handle is a lock guard
            quote! {let mut slice = self.data.write_local_data().await; }, //this is the lock
        )
    } else if array_type == "ReadOnlyArray" {
        (
            quote! {}, //no explicit lock since the slice handle is a lock guard
            quote! {let slice = self.data.local_data();}, //this is the lock
        )
    } else {
        (
            quote! {}, //no lock cause either readonly or unsafe
            quote! {let mut slice = unsafe{self.data.mut_local_data()};},
        )
    };

    let multi_val_multi_idx_match_stmts = quote! {};
    let single_val_multi_idx_match_stmts = quote! {};
    let multi_val_single_idx_match_stmts = quote! {};
    let mut all_match_stmts: Vec<(
        proc_macro2::TokenStream,
        fn(
            proc_macro2::TokenStream,
            &proc_macro2::TokenStream,
            proc_macro2::TokenStream,
        ) -> proc_macro2::TokenStream,
    )> = vec![
        (multi_val_multi_idx_match_stmts, gen_multi_val_multi_idx),
        (single_val_multi_idx_match_stmts, gen_single_val_multi_idx),
        (multi_val_single_idx_match_stmts, gen_multi_val_single_idx),
    ];
    for (match_stmts, gen_fn) in all_match_stmts.iter_mut() {
        for optype in optypes {
            match optype {
                OpType::Arithmetic => {
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::Add},
                        &lock,
                        quote! { #lhs += val; },
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::Sub},
                        &lock,
                        quote! {#lhs -= val; },
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::Mul},
                        &lock,
                        quote! {#lhs *= val;},
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::Div},
                        &lock,
                        quote! {#lhs /= val; },
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::Rem},
                        &lock,
                        quote! {#lhs %= val; },
                    ));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd::Put}, &lock, assign.clone()));
                }
                OpType::Bitwise => {
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::And},
                        &lock,
                        quote! {#lhs &= val; },
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::Or},
                        &lock,
                        quote! {#lhs |= val; },
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::Xor},
                        &lock,
                        quote! {#lhs ^= val; },
                    ));
                }
                OpType::Access => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd::Store}, &lock, assign.clone()));
                }
                OpType::Shift => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd::Shl}, &lock, shl.clone()));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd::Shr}, &lock, shr.clone()));
                }
                _ => {} //for fetch, readonly, and compex ops do nothing
            }
        }
        match_stmts.extend(quote! {
            _=> unreachable!("op: {:?} should not be possible in this context", self.op),
        });
    }

    let multi_val_multi_idx_match_stmts = all_match_stmts[0].0.clone();
    let single_val_multi_idx_match_stmts = all_match_stmts[1].0.clone();
    let multi_val_single_idx_match_stmts = all_match_stmts[2].0.clone();

    let multi_val_multi_idx_fetch_match_stmts = quote! {};
    let single_val_multi_idx_fetch_match_stmts = quote! {};
    let multi_val_single_idx_fetch_match_stmts = quote! {};
    let mut all_match_stmts: Vec<(
        proc_macro2::TokenStream,
        fn(
            proc_macro2::TokenStream,
            &proc_macro2::TokenStream,
            proc_macro2::TokenStream,
        ) -> proc_macro2::TokenStream,
    )> = vec![
        (
            multi_val_multi_idx_fetch_match_stmts,
            gen_multi_val_multi_idx,
        ),
        (
            single_val_multi_idx_fetch_match_stmts,
            gen_single_val_multi_idx,
        ),
        (
            multi_val_single_idx_fetch_match_stmts,
            gen_multi_val_single_idx,
        ),
    ];
    for (match_stmts, gen_fn) in all_match_stmts.iter_mut() {
        for optype in optypes {
            match optype {
                OpType::Arithmetic => {
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::FetchAdd},
                        &lock,
                        fetch_add.clone(),
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::FetchSub},
                        &lock,
                        fetch_sub.clone(),
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::FetchMul},
                        &lock,
                        fetch_mul.clone(),
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::FetchDiv},
                        &lock,
                        fetch_div.clone(),
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::FetchRem},
                        &lock,
                        fetch_rem.clone(),
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::Get},
                        &lock,
                        quote! {res.push(#load);},
                    ));
                }
                OpType::Bitwise => {
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::FetchAnd},
                        &lock,
                        fetch_and.clone(),
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::FetchOr},
                        &lock,
                        fetch_or.clone(),
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::FetchXor},
                        &lock,
                        fetch_xor.clone(),
                    ));
                }
                OpType::Access => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd::Swap}, &lock, swap.clone()));
                }
                OpType::ReadOnly => {
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::Load},
                        &lock,
                        quote! {res.push(#load);},
                    ));
                }
                OpType::Shift => {
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::FetchShl},
                        &lock,
                        fetch_shl.clone(),
                    ));
                    match_stmts.extend(gen_fn(
                        quote! {ArrayOpCmd::FetchShr},
                        &lock,
                        fetch_shr.clone(),
                    ));
                }
                _ => {} //dont handle result ops (CompEx,CompExEs) here
            }
        }
        match_stmts.extend(quote! {
            _=> unreachable!("op: {:?} should not be possible in this context", self.op),
        });
    }

    let multi_val_multi_idx_fetch_match_stmts = all_match_stmts[0].0.clone();
    let single_val_multi_idx_fetch_match_stmts = all_match_stmts[1].0.clone();
    let multi_val_single_idx_fetch_match_stmts = all_match_stmts[2].0.clone();

    let multi_val_multi_idx_result_match_stmts = quote! {};
    let single_val_multi_idx_result_match_stmts = quote! {};
    let multi_val_single_idx_result_match_stmts = quote! {};
    let mut all_match_stmts: Vec<(
        proc_macro2::TokenStream,
        fn(
            proc_macro2::TokenStream,
            &proc_macro2::TokenStream,
            proc_macro2::TokenStream,
        ) -> proc_macro2::TokenStream,
    )> = vec![
        (
            multi_val_multi_idx_result_match_stmts,
            gen_multi_val_multi_idx,
        ),
        (
            single_val_multi_idx_result_match_stmts,
            gen_single_val_multi_idx,
        ),
        (
            multi_val_single_idx_result_match_stmts,
            gen_multi_val_single_idx,
        ),
    ];
    for (match_stmts, gen_fn) in all_match_stmts.iter_mut() {
        for optype in optypes {
            match optype {
                OpType::CompEx => match_stmts.extend(gen_fn(
                    quote! {ArrayOpCmd::CompareExchange(old)},
                    &lock,
                    compare_exchange.clone(),
                )),
                OpType::CompExEps => match_stmts.extend(gen_fn(
                    quote! {ArrayOpCmd::CompareExchangeEps(old,eps)},
                    &lock,
                    compare_exchange_eps.clone(),
                )),
                _ => {} //current only ops that return results are CompEx, CompExEps
            }
        }
        match_stmts.extend(quote! {
            _=> unreachable!("op: {:?} should not be possible in this context", self.op),
        });
    }

    let multi_val_multi_idx_result_match_stmts = all_match_stmts[0].0.clone();
    let single_val_multi_idx_result_match_stmts = all_match_stmts[1].0.clone();
    let multi_val_single_idx_result_match_stmts = all_match_stmts[2].0.clone();

    let (
        multi_val_multi_idx_am_buf_name,
        dist_multi_val_multi_idx_am_buf_name,
        multi_val_multi_idx_am_buf_fetch_name,
        dist_multi_val_multi_idx_am_buf_fetch_name,
        multi_val_multi_idx_am_buf_result_name,
        dist_multi_val_multi_idx_am_buf_result_name,
        multi_val_multi_idx_reg_name,
        multi_val_multi_idx_id,
    ) = gen_array_names(&array_type, &typeident, "multi", "multi");

    let (
        single_val_multi_idx_am_buf_name,
        dist_single_val_multi_idx_am_buf_name,
        single_val_multi_idx_am_buf_fetch_name,
        dist_single_val_multi_idx_am_buf_fetch_name,
        single_val_multi_idx_am_buf_result_name,
        dist_single_val_multi_idx_am_buf_result_name,
        single_val_multi_idx_reg_name,
        single_val_multi_idx_id,
    ) = gen_array_names(&array_type, &typeident, "single", "multi");

    let (
        multi_val_single_idx_am_buf_name,
        dist_multi_val_single_idx_am_buf_name,
        multi_val_single_idx_am_buf_fetch_name,
        dist_multi_val_single_idx_am_buf_fetch_name,
        multi_val_single_idx_am_buf_result_name,
        dist_multi_val_single_idx_am_buf_result_name,
        multi_val_single_idx_reg_name,
        multi_val_single_idx_id,
    ) = gen_array_names(&array_type, &typeident, "multi", "single");

    let serde_bytes = format! {"{}::serde_bytes",lamellar};
    if array_type != "ReadOnlyArray" {
        // Updating ops that dont return anything
        expanded.extend(quote! {
            #[allow(non_camel_case_types)]
            #[#am_data(Debug)]
            struct #multi_val_multi_idx_am_buf_name{
                data: #lamellar::array::#array_type<#typeident>,
                op: #lamellar::array::ArrayOpCmd<#typeident>,
                #[serde(with = #serde_bytes)]
                idx_vals: Vec<u8>,
                index_size: u8,
            }
            #[#am]
            impl LamellarAM for #multi_val_multi_idx_am_buf_name{ //eventually we can return fetchs here too...
                async fn exec(&self) {
                    // println!("in multi val multi idx exec");
                    #slice
                    match self.index_size{
                        1 => {
                            let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u8,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u8,#typeident>>())};
                            match self.op {
                                #multi_val_multi_idx_match_stmts
                            }
                        }
                        2 => {
                            let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u16,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u16,#typeident>>())};
                            match self.op {
                                #multi_val_multi_idx_match_stmts
                            }
                        }
                        4 => {
                            let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u32,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u32,#typeident>>())};
                            match self.op {
                                #multi_val_multi_idx_match_stmts
                            }
                        }
                        8 => {
                            let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u64,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u64,#typeident>>())};
                            match self.op {
                                #multi_val_multi_idx_match_stmts
                            }
                        }
                        _ => {
                            let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<usize,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<usize,#typeident>>())};
                            match self.op {
                                #multi_val_multi_idx_match_stmts
                            }
                        }
                    };
                }
            }
            #[allow(non_snake_case)]
            fn #dist_multi_val_multi_idx_am_buf_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, idx_vals: Vec<u8>, index_size: u8) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                    Arc::new(#multi_val_multi_idx_am_buf_name{
                        data: Into::into(array),
                        op: op.into(),
                        idx_vals: idx_vals,
                        index_size: index_size,
                    })
            }

            inventory::submit! {
                #lamellar::array::#multi_val_multi_idx_reg_name{
                    id: #multi_val_multi_idx_id,
                    batch_type: #lamellar::array::BatchReturnType::None,
                    op: #dist_multi_val_multi_idx_am_buf_name,
                }
            }

            #[allow(non_camel_case_types)]
            #[#am_data(Debug)]
            struct #single_val_multi_idx_am_buf_name{
                data: #lamellar::array::#array_type<#typeident>,
                op: #lamellar::array::ArrayOpCmd<#typeident>,
                val: #typeident,
                #[serde(with = #serde_bytes)]
                indices: Vec<u8>,
                index_size: u8,
            }
            #[#am]
            impl LamellarAM for #single_val_multi_idx_am_buf_name{ //eventually we can return fetchs here too...
                async fn exec(&self) {
                    // println!("in single val multi idx exec");
                    // let mut timer = std::time::Instant::now();
                    #slice
                    // println!("get slice time: {}",timer.elapsed().as_secs_f64());
                    // timer = std::time::Instant::now();
                    let val = self.val;
                    match self.index_size{
                        1 => {
                            let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u8, self.indices.len()/std::mem::size_of::<u8>())};
                            // println!("Indices: {:?}",indices);
                            match self.op {
                                #single_val_multi_idx_match_stmts
                            }
                        }
                        2 => {
                            let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u16, self.indices.len()/std::mem::size_of::<u16>())};
                            match self.op {
                                #single_val_multi_idx_match_stmts
                            }
                        }
                        4 => {
                            let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u32, self.indices.len()/std::mem::size_of::<u32>())};
                            match self.op {
                                #single_val_multi_idx_match_stmts
                            }
                        }
                        8 => {
                            let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u64, self.indices.len()/std::mem::size_of::<u64>())};
                            match self.op {
                                #single_val_multi_idx_match_stmts
                            }
                        }
                        _ => {
                            let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const usize, self.indices.len()/std::mem::size_of::<usize>())};
                            match self.op {
                                #single_val_multi_idx_match_stmts
                            }
                        }
                    }
                    // println!("op time: {}",timer.elapsed().as_secs_f64());
                }
            }
            #[allow(non_snake_case)]
            fn #dist_single_val_multi_idx_am_buf_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, val: Vec<u8>, indicies: Vec<u8>, index_size: u8) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                    let val_slice = unsafe {std::slice::from_raw_parts(val.as_ptr() as *const #typeident, std::mem::size_of::<#typeident>())};
                    let val = val_slice[0];
                    Arc::new(#single_val_multi_idx_am_buf_name{
                        data: Into::into(array),
                        op: op.into(),
                        val: val,
                        indices: indicies,
                        index_size: index_size,
                    })
            }
            inventory::submit! {
                #lamellar::array::#single_val_multi_idx_reg_name{
                    id: #single_val_multi_idx_id,
                    batch_type: #lamellar::array::BatchReturnType::None,
                    op: #dist_single_val_multi_idx_am_buf_name,
                }
            }

            #[allow(non_camel_case_types)]
            #[#am_data(Debug)]
            struct #multi_val_single_idx_am_buf_name{
                data: #lamellar::array::#array_type<#typeident>,
                op: #lamellar::array::ArrayOpCmd<#typeident>,
                #[serde(with = #serde_bytes)]
                vals: Vec<u8>,
                index: usize,
            }
            #[#am]
            impl LamellarAM for #multi_val_single_idx_am_buf_name{ //eventually we can return fetchs here too...
                async fn exec(&self) {
                    // println!("in multi val single idx exec");
                    #slice
                    let vals = unsafe {std::slice::from_raw_parts(self.vals.as_ptr() as *const #typeident, self.vals.len()/std::mem::size_of::<#typeident>())};
                    let index = self.index;
                    match self.op {
                        #multi_val_single_idx_match_stmts
                    }
                }
            }
            #[allow(non_snake_case)]
            fn #dist_multi_val_single_idx_am_buf_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, vals: Vec<u8>, index: usize) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                    Arc::new(#multi_val_single_idx_am_buf_name{
                        data: Into::into(array),
                        op: op.into(),
                        vals: vals,
                        index: index,
                    })
            }
            inventory::submit! {
                #lamellar::array::#multi_val_single_idx_reg_name{
                    id: #multi_val_single_idx_id,
                    batch_type: #lamellar::array::BatchReturnType::None,
                    op: #dist_multi_val_single_idx_am_buf_name,
                }
            }
        });

        // ops that return a result
        if optypes.contains(&OpType::CompEx) || optypes.contains(&OpType::CompExEps) {
            expanded.extend(quote! {
                #[allow(non_camel_case_types)]
                #[#am_data(Debug)]
                struct #multi_val_multi_idx_am_buf_result_name{
                    data: #lamellar::array::#array_type<#typeident>,
                    op: #lamellar::array::ArrayOpCmd<#typeident>,
                    #[serde(with = #serde_bytes)]
                    idx_vals: Vec<u8>,
                    index_size: u8,
                }
                #[#am]
                impl LamellarAM for #multi_val_multi_idx_am_buf_result_name{ //eventually we can return fetchs here too...
                    async fn exec(&self) -> #lamellar::memregion::OneSidedMemoryRegion<u8> {
                        // println!("in multi val multi idx result exec");
                        #slice
                        let mut res = Vec::new();
                        match self.index_size{
                            1 => {
                                let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u8,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u8,#typeident>>())};
                                match self.op {
                                    #multi_val_multi_idx_result_match_stmts
                                }
                            }
                            2 => {
                                let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u16,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u16,#typeident>>())};
                                match self.op {
                                    #multi_val_multi_idx_result_match_stmts
                                }
                            }
                            4 => {
                                let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u32,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u32,#typeident>>())};
                                match self.op {
                                    #multi_val_multi_idx_result_match_stmts
                                }
                            }
                            8 => {
                                let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u64,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u64,#typeident>>())};
                                match self.op {
                                    #multi_val_multi_idx_result_match_stmts
                                }
                            }
                            _ => {
                                let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<usize,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<usize,#typeident>>())};
                                match self.op {
                                    #multi_val_multi_idx_result_match_stmts
                                }
                            }
                        };
                        let byte_len = res.len() * std::mem::size_of::<Result<#typeident,#typeident>>();
                        let mut mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                        while mem_region.is_err() {
                            async_std::task::yield_now().await;
                            mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                        }
                        let mem_region = mem_region.unwrap();
                        unsafe {
                            let res_bytes = std::slice::from_raw_parts(res.as_ptr() as *const u8, byte_len);
                            mem_region.local_copy_from_slice(res_bytes);
                        }
                        mem_region
                    }
                }
                #[allow(non_snake_case)]
                fn #dist_multi_val_multi_idx_am_buf_result_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, idx_vals: Vec<u8>, index_size: u8) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                        Arc::new(#multi_val_multi_idx_am_buf_result_name{
                            data: Into::into(array),
                            op: op.into(),
                            idx_vals: idx_vals,
                            index_size: index_size,
                        })
                }
                inventory::submit! {
                    #lamellar::array::#multi_val_multi_idx_reg_name{
                        id: #multi_val_multi_idx_id,
                        batch_type: #lamellar::array::BatchReturnType::Result,
                        op: #dist_multi_val_multi_idx_am_buf_result_name,
                    }
                }

                #[allow(non_camel_case_types)]
                #[#am_data(Debug)]
                struct #single_val_multi_idx_am_buf_result_name{
                    data: #lamellar::array::#array_type<#typeident>,
                    op: #lamellar::array::ArrayOpCmd<#typeident>,
                    val: #typeident,
                    #[serde(with = #serde_bytes)]
                    indices: Vec<u8>,
                    index_size: u8,
                }
                #[#am]
                impl LamellarAM for #single_val_multi_idx_am_buf_result_name{ //eventually we can return fetchs here too...
                    async fn exec(&self) -> #lamellar::memregion::OneSidedMemoryRegion<u8> {
                        // println!("in single val multi idx result exec");
                        #slice
                        let val = self.val;
                        let mut res = Vec::new();
                        match self.index_size{
                            1 => {
                                let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u8, self.indices.len()/std::mem::size_of::<u8>())};
                                match self.op {
                                    #single_val_multi_idx_result_match_stmts
                                }
                            }
                            2 => {
                                let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u16, self.indices.len()/std::mem::size_of::<u16>())};
                                match self.op {
                                    #single_val_multi_idx_result_match_stmts
                                }
                            }
                            4 => {
                                let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u32, self.indices.len()/std::mem::size_of::<u32>())};
                                match self.op {
                                    #single_val_multi_idx_result_match_stmts
                                }
                            }
                            8 => {
                                let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u64, self.indices.len()/std::mem::size_of::<u64>())};
                                match self.op {
                                    #single_val_multi_idx_result_match_stmts
                                }
                            }
                            _ => {
                                let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const usize, self.indices.len()/std::mem::size_of::<usize>())};
                                match self.op {
                                    #single_val_multi_idx_result_match_stmts
                                }
                            }
                        }
                        // println!("done in in single val multi idx result exec");
                        let byte_len = res.len() * std::mem::size_of::<Result<#typeident,#typeident>>();
                        let mut mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                        while mem_region.is_err() {
                            async_std::task::yield_now().await;
                            mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                        }
                        let mem_region = mem_region.unwrap();
                        unsafe {
                            let res_bytes = std::slice::from_raw_parts(res.as_ptr() as *const u8, byte_len);
                            mem_region.local_copy_from_slice(res_bytes);
                        }
                        mem_region
                    }
                }
                #[allow(non_snake_case)]
                fn #dist_single_val_multi_idx_am_buf_result_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, val: Vec<u8>, indicies: Vec<u8>, index_size: u8) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                        let val_slice = unsafe {std::slice::from_raw_parts(val.as_ptr() as *const #typeident, std::mem::size_of::<#typeident>())};
                        let val = val_slice[0];
                        Arc::new(#single_val_multi_idx_am_buf_result_name{
                            data: Into::into(array),
                            op: op.into(),
                            val: val,
                            indices: indicies,
                            index_size: index_size,
                        })
                }
                inventory::submit! {
                    #lamellar::array::#single_val_multi_idx_reg_name{
                        id: #single_val_multi_idx_id,
                        batch_type: #lamellar::array::BatchReturnType::Result,
                        op: #dist_single_val_multi_idx_am_buf_result_name,
                    }
                }

                #[allow(non_camel_case_types)]
                #[#am_data(Debug)]
                struct #multi_val_single_idx_am_buf_result_name{
                    data: #lamellar::array::#array_type<#typeident>,
                    op: #lamellar::array::ArrayOpCmd<#typeident>,
                    #[serde(with = #serde_bytes)]
                    vals: Vec<u8>,
                    index: usize,
                }
                #[#am]
                impl LamellarAM for #multi_val_single_idx_am_buf_result_name{ //eventually we can return fetchs here too...
                    async fn exec(&self) -> #lamellar::memregion::OneSidedMemoryRegion<u8> {
                        // println!("in multi val single idx result exec");
                        #slice
                        let vals = unsafe {std::slice::from_raw_parts(self.vals.as_ptr() as *const #typeident, self.vals.len()/std::mem::size_of::<#typeident>())};
                        let index = self.index;
                        let mut res = Vec::new();
                        match self.op {
                            #multi_val_single_idx_result_match_stmts
                        }
                        let byte_len = res.len() * std::mem::size_of::<Result<#typeident,#typeident>>();
                        let mut mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                        while mem_region.is_err() {
                            async_std::task::yield_now().await;
                            mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                        }
                        let mem_region = mem_region.unwrap();
                        unsafe {
                            let res_bytes = std::slice::from_raw_parts(res.as_ptr() as *const u8, byte_len);
                            mem_region.local_copy_from_slice(res_bytes);
                        }
                        mem_region
                    }
                }
                #[allow(non_snake_case)]
                fn #dist_multi_val_single_idx_am_buf_result_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, vals: Vec<u8>, index: usize) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                        Arc::new(#multi_val_single_idx_am_buf_result_name{
                            data: Into::into(array),
                            op: op.into(),
                            vals: vals,
                            index: index,
                        })
                }
                inventory::submit! {
                    #lamellar::array::#multi_val_single_idx_reg_name{
                        id: #multi_val_single_idx_id,
                        batch_type: #lamellar::array::BatchReturnType::Result,
                        op: #dist_multi_val_single_idx_am_buf_result_name,
                    }
                }
            });
        }
    }
    //ops that return a value

    // println!("creating the access stuff");
    expanded.extend(quote! {
        #[allow(non_camel_case_types)]
        #[#am_data(Debug)]
        struct #multi_val_multi_idx_am_buf_fetch_name{
            data: #lamellar::array::#array_type<#typeident>,
            op: #lamellar::array::ArrayOpCmd<#typeident>,
            #[serde(with = #serde_bytes)]
            idx_vals: Vec<u8>,
            index_size: u8,
        }
        #[#am]
        impl LamellarAM for #multi_val_multi_idx_am_buf_fetch_name{ //eventually we can return fetchs here too...
            async fn exec(&self) -> #lamellar::memregion::OneSidedMemoryRegion<u8> {
                // println!("in multi val multi idx fetch exec");
                #slice
                let mut res = Vec::new();
                match self.index_size{
                    1 => {
                        let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u8,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u8,#typeident>>())};
                        match self.op {
                            #multi_val_multi_idx_fetch_match_stmts
                        }
                    }
                    2 => {
                        let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u16,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u16,#typeident>>())};
                        match self.op {
                            #multi_val_multi_idx_fetch_match_stmts
                        }
                    }
                    4 => {
                        let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u32,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u32,#typeident>>())};
                        match self.op {
                            #multi_val_multi_idx_fetch_match_stmts
                        }
                    }
                    8 => {
                        let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<u64,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<u64,#typeident>>())};
                        match self.op {
                            #multi_val_multi_idx_fetch_match_stmts
                        }
                    }
                    _ => {
                        let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<usize,#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<usize,#typeident>>())};
                        match self.op {
                            #multi_val_multi_idx_fetch_match_stmts
                        }
                    }
                };
                let byte_len = res.len() * std::mem::size_of::<#typeident>();
                let mut mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                while mem_region.is_err() {
                    async_std::task::yield_now().await;
                    mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                }
                let mem_region = mem_region.unwrap();
                unsafe {
                    let res_bytes = std::slice::from_raw_parts(res.as_ptr() as *const u8, byte_len);
                    mem_region.local_copy_from_slice(res_bytes);
                }
                mem_region
            }
        }
        #[allow(non_snake_case)]
        fn #dist_multi_val_multi_idx_am_buf_fetch_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, idx_vals: Vec<u8>,index_usize: u8) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                Arc::new(#multi_val_multi_idx_am_buf_fetch_name{
                    data: Into::into(array),
                    op: op.into(),
                    idx_vals: idx_vals,
                    index_size: index_usize,
                })
        }
        fn #multi_val_multi_idx_id (batch_type:  #lamellar::array::BatchReturnType) -> (std::any::TypeId,std::any::TypeId,#lamellar::array::BatchReturnType) {
            // println!("in multi_val_multi_idx_id {} {}",stringify!(#typeident), stringify!(#byte_array_type));
            (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),batch_type)
        }
        inventory::submit! {
            #lamellar::array::#multi_val_multi_idx_reg_name{
                id: #multi_val_multi_idx_id,
                batch_type: #lamellar::array::BatchReturnType::Vals,
                op: #dist_multi_val_multi_idx_am_buf_fetch_name,
            }
        }

        #[allow(non_camel_case_types)]
        #[#am_data(Debug)]
        struct #single_val_multi_idx_am_buf_fetch_name{
            data: #lamellar::array::#array_type<#typeident>,
            op: #lamellar::array::ArrayOpCmd<#typeident>,
            val: #typeident,
            #[serde(with = #serde_bytes)]
            indices: Vec<u8>,
            index_size: u8,
        }
        #[#am]
        impl LamellarAM for #single_val_multi_idx_am_buf_fetch_name{ //eventually we can return fetchs here too...
            async fn exec(&self) -> #lamellar::memregion::OneSidedMemoryRegion<u8> {
                // println!("in single val multi idx fetch exec");
                #slice
                let val = self.val;
                let mut res;
                match self.index_size{
                    1 => {
                        let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u8, self.indices.len()/std::mem::size_of::<u8>())};
                        res = Vec::with_capacity(self.indices.len()/std::mem::size_of::<u8>());
                        // println!("indices: {:?}", indices);
                        match self.op {
                            #single_val_multi_idx_fetch_match_stmts
                        }
                    }
                    2 => {
                        let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u16, self.indices.len()/std::mem::size_of::<u16>())};
                        res = Vec::with_capacity(self.indices.len()/std::mem::size_of::<u16>());
                        match self.op {
                            #single_val_multi_idx_fetch_match_stmts
                        }
                    }
                    4 => {
                        let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u32, self.indices.len()/std::mem::size_of::<u32>())};
                        res = Vec::with_capacity(self.indices.len()/std::mem::size_of::<u32>());
                        match self.op {
                            #single_val_multi_idx_fetch_match_stmts
                        }
                    }
                    8 => {
                        let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const u64, self.indices.len()/std::mem::size_of::<u64>())};
                        res = Vec::with_capacity(self.indices.len()/std::mem::size_of::<u64>());
                        match self.op {
                            #single_val_multi_idx_fetch_match_stmts
                        }
                    }
                    _ => {
                        let indices = unsafe {std::slice::from_raw_parts(self.indices.as_ptr() as *const usize, self.indices.len()/std::mem::size_of::<usize>())};
                        res = Vec::with_capacity(self.indices.len()/std::mem::size_of::<usize>());
                        match self.op {
                            #single_val_multi_idx_fetch_match_stmts
                        }
                    }
                }
                // println!("done with exec");
                let byte_len = res.len() * std::mem::size_of::<#typeident>();
                let mut mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                while mem_region.is_err() {
                    async_std::task::yield_now().await;
                    mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                }
                let mem_region = mem_region.unwrap();
                unsafe {
                    let res_bytes = std::slice::from_raw_parts(res.as_ptr() as *const u8, byte_len);
                    mem_region.local_copy_from_slice(res_bytes);
                }
                mem_region
            }
        }
        #[allow(non_snake_case)]
        fn #dist_single_val_multi_idx_am_buf_fetch_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, val: Vec<u8>, indicies: Vec<u8>,index_size: u8) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                let val_slice = unsafe {std::slice::from_raw_parts(val.as_ptr() as *const #typeident, std::mem::size_of::<#typeident>())};
                let val = val_slice[0];
                Arc::new(#single_val_multi_idx_am_buf_fetch_name{
                    data: Into::into(array),
                    op: op.into(),
                    val: val,
                    indices: indicies,
                    index_size: index_size,
                })
        }
        fn #single_val_multi_idx_id (batch_type:  #lamellar::array::BatchReturnType) -> (std::any::TypeId,std::any::TypeId, #lamellar::array::BatchReturnType) {
            (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),batch_type)
        }
        inventory::submit! {
            #lamellar::array::#single_val_multi_idx_reg_name{
                id: #single_val_multi_idx_id,
                batch_type: #lamellar::array::BatchReturnType::Vals,
                op: #dist_single_val_multi_idx_am_buf_fetch_name,
            }
        }

        #[allow(non_camel_case_types)]
        #[#am_data(Debug)]
        struct #multi_val_single_idx_am_buf_fetch_name{
            data: #lamellar::array::#array_type<#typeident>,
            op: #lamellar::array::ArrayOpCmd<#typeident>,
            #[serde(with = #serde_bytes)]
            vals: Vec<u8>,
            index: usize,
        }
        #[#am]
        impl LamellarAM for #multi_val_single_idx_am_buf_fetch_name{ //eventually we can return fetchs here too...
            async fn exec(&self) -> #lamellar::memregion::OneSidedMemoryRegion<u8> {
                // println!("in multi val single idx fetch exec");
                #slice
                let vals = unsafe {std::slice::from_raw_parts(self.vals.as_ptr() as *const #typeident, self.vals.len()/std::mem::size_of::<#typeident>())};
                let index = self.index;
                let mut res = Vec::new();
                match self.op {
                    #multi_val_single_idx_fetch_match_stmts
                }
                // println!("res: {:?}",res);
                let byte_len = res.len() * std::mem::size_of::<#typeident>();
                let mut mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                while mem_region.is_err() {
                    async_std::task::yield_now().await;
                    mem_region = self.data.team().try_alloc_one_sided_mem_region(byte_len);
                }
                let mem_region = mem_region.unwrap();
                unsafe {
                    let res_bytes = std::slice::from_raw_parts(res.as_ptr() as *const u8, byte_len);
                    mem_region.local_copy_from_slice(res_bytes);
                }
                mem_region
            }
        }
        #[allow(non_snake_case)]
        fn #dist_multi_val_single_idx_am_buf_fetch_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, vals: Vec<u8>, index: usize) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                Arc::new(#multi_val_single_idx_am_buf_fetch_name{
                    data: Into::into(array),
                    op: op.into(),
                    vals: vals,
                    index: index,
                })
        }
        fn #multi_val_single_idx_id (batch_type:  #lamellar::array::BatchReturnType) -> (std::any::TypeId,std::any::TypeId, #lamellar::array::BatchReturnType) {
            (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),batch_type)
        }
        inventory::submit! {
            #lamellar::array::#multi_val_single_idx_reg_name{
                id: #multi_val_single_idx_id,
                batch_type: #lamellar::array::BatchReturnType::Vals,
                op: #dist_multi_val_single_idx_am_buf_fetch_name,
            }
        }
    });

    expanded
}

#[derive(Debug, Clone, std::cmp::PartialEq)]
enum OpType {
    Arithmetic,
    Bitwise,
    Access,
    CompEx,
    CompExEps,
    ReadOnly,
    Shift,
}

fn create_buffered_ops(
    typeident: syn::Type,
    optypes: Vec<OpType>,
    native: bool,
    rt: bool,
) -> proc_macro2::TokenStream {
    let mut atomic_array_types: Vec<(syn::Ident, syn::Ident)> = vec![
        (
            quote::format_ident!("LocalLockArray"),
            quote::format_ident!("__LocalLockByteArray"),
        ),
        (
            quote::format_ident!("GlobalLockArray"),
            quote::format_ident!("__GlobalLockByteArray"),
        ),
    ];

    if native {
        atomic_array_types.push((
            quote::format_ident!("NativeAtomicArray"),
            quote::format_ident!("__NativeAtomicByteArray"),
        ));
    } else {
        atomic_array_types.push((
            quote::format_ident!("GenericAtomicArray"),
            quote::format_ident!("__GenericAtomicByteArray"),
        ));
    }

    let mut expanded = quote! {};

    let ro_optypes = vec![OpType::ReadOnly]; //, vec![OpType::Arithmetic, OpType::Access];

    let buf_op_impl = create_buf_ops(
        typeident.clone(),
        quote::format_ident!("ReadOnlyArray"),
        quote::format_ident!("__ReadOnlyByteArray"),
        &ro_optypes,
        rt,
    );
    expanded.extend(buf_op_impl);

    let buf_op_impl = create_buf_ops(
        typeident.clone(),
        quote::format_ident!("UnsafeArray"),
        quote::format_ident!("__UnsafeByteArray"),
        &optypes,
        rt,
    );
    expanded.extend(buf_op_impl);

    for (array_type, byte_array_type) in atomic_array_types {
        let buf_op_impl = create_buf_ops(
            typeident.clone(),
            array_type.clone(),
            byte_array_type.clone(),
            &optypes,
            rt,
        );
        expanded.extend(buf_op_impl);
    }

    expanded
}

pub(crate) fn __derive_arrayops(input: TokenStream) -> TokenStream {
    // println!("__derive_arrayops called");
    let input = parse_macro_input!(input as syn::DeriveInput);
    let name = input.ident.clone();
    let the_type: syn::Type = syn::parse_quote!(#name);

    let mut op_types = vec![OpType::ReadOnly, OpType::Access];
    let mut opt_op_types = vec![OpType::ReadOnly, OpType::Access];
    let mut element_wise_trait_impls = quote! {

        // impl Dist for Option<#the_type> {} // only traits defined in the current crate can be implemented for types defined outside of the crate
                                              // so trying to implement for Option<#the_type> fails cause its in the users crate and not actually in lamellar
                                              // need to research if there is a way around this...
    };

    for attr in &input.attrs {
        if attr.path().is_ident("array_ops") {
            // println!("array_ops attr found");
            attr.parse_nested_meta(|temp| {
                if temp.path.is_ident("Arithmetic") {
                    op_types.push(OpType::Arithmetic);
                    element_wise_trait_impls.extend(
                        quote! {
                            impl __lamellar::ElementArithmeticOps for #the_type {}
                        }
                    );
                    Ok(())
                }
                else if temp.path.is_ident("CompExEps") {
                    op_types.push(OpType::CompExEps);
                    opt_op_types.push(OpType::CompExEps);
                    element_wise_trait_impls.extend(
                        quote! {
                            impl __lamellar::ElementComparePartialEqOps for #the_type {}
                        }
                    );
                    Ok(())
                }
                else if temp.path.is_ident("CompEx") {
                    op_types.push(OpType::CompEx);
                    opt_op_types.push(OpType::CompEx);
                    element_wise_trait_impls.extend(
                        quote! {
                            impl __lamellar::ElementCompareEqOps for #the_type {}
                            // impl __lamellar::ElementCompareEqOps for Option< #the_type > {} //see note above why we cant do this
                        }
                    );
                    Ok(())
                }
                else if temp.path.is_ident("Bitwise") {
                    op_types.push(OpType::Bitwise);
                    element_wise_trait_impls.extend(
                        quote! {
                            impl __lamellar::ElementBitWiseOps for #the_type {}
                        }
                    );
                    Ok(())
                }
                else if temp.path.is_ident("Shift") {
                    op_types.push(OpType::Shift);
                    element_wise_trait_impls.extend(
                        quote! {
                            impl __lamellar::ElementShiftOps for #the_type {}
                        }
                    );
                    Ok(())
                }
                else if temp.path.is_ident("All") {
                    op_types.push(OpType::Arithmetic);
                    op_types.push(OpType::CompEx);
                    op_types.push(OpType::CompExEps);
                    op_types.push(OpType::Bitwise);
                    op_types.push(OpType::Shift);

                    // opt_op_types.push(OpType::CompEx); //see note above why we cant do this
                    // opt_op_types.push(OpType::CompExEps); //see note above why we cant do this

                    element_wise_trait_impls.extend(
                        quote! {
                            impl __lamellar::ElementArithmeticOps for #the_type {}
                            impl __lamellar::ElementComparePartialEqOps for #the_type {}
                            impl __lamellar::ElementCompareEqOps for #the_type {}
                            impl __lamellar::ElementBitWiseOps for #the_type {}
                            impl __lamellar::ElementShiftOps for #the_type {}

                            // impl __lamellar::ElementComparePartialEqOps for Option< #the_type > {} //see note above why we cant do this
                            // impl __lamellar::ElementCompareEqOps for Option< #the_type > {} //see note above why we cant do this

                        }
                    );
                    Ok(())
                }
                else {
                    Err(temp.error("unexpected array op type, valid types are: Arithmetic, CompEx, CompExEps, Bitwise, Shift, All"))
                }
                                // &_ => abort!(item, "unexpected array op type, valid types are: Arithmetic, CompEx, CompExEps, Bitwise, Shift, All"),
                            // }
                //         }
                //     }
                // }
            }).unwrap();
        }
    }
    let buf_ops = create_buffered_ops(the_type.clone(), op_types, false, false);
    // let opt_type = syn::parse_str(&format!("Option<{}>", the_type.to_token_stream())).unwrap(); //see note above why we cant do this
    // let opt_buf_opt = create_buffered_ops(opt_type, opt_op_types, false, false); //see note above why we cant do this

    let output = quote_spanned! {input.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            use __lamellar::array::prelude::*;
            use __lamellar::active_messaging::prelude::*;
            use __lamellar::memregion::prelude::*;
            use __lamellar::darc::prelude::*;
            use __lamellar::array::{
                ArrayOpCmd,
                IdxVal,
                __ReadOnlyByteArray,
                __UnsafeByteArray,
                __LocalLockByteArray,
                __GlobalLockByteArray,
                __GenericAtomicByteArray,
            };
            use __lamellar::active_messaging::RemoteActiveMessage;

            use __lamellar::parking_lot::{Mutex,RwLock};
            // use __lamellar::tracing::*;
            use __lamellar::async_trait;
            use __lamellar::inventory;
            use std::sync::Arc;
            use std::sync::atomic::{Ordering,AtomicBool,AtomicUsize};
            use std::pin::Pin;

            impl __lamellar::_ArrayOps for #the_type {}
            // impl __lamellar::_ArrayOps for Option< #the_type > {} //see note above why we cant do this
            #element_wise_trait_impls
            #buf_ops
            // #opt_buf_opt //see note above why we cant do this
        };
    };
    TokenStream::from(output)
}
