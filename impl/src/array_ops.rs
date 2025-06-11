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

fn native_atomic_slice(
    typeident: &syn::Type,
    lamellar: &proc_macro2::Ident,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let ident = match typeident {
        syn::Type::Path(path) => path
            .path
            .get_ident()
            .expect("unexpected type for native atomic"),
        _ => panic!("unexpected type for native atomic"),
    };
    match ident.to_string().as_str() {
        "i8" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicI8;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicI8(&slice[index]);
                a_val
            },
        ),
        "i16" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicI16;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicI16(&slice[index]);
                a_val
            },
        ),
        "i32" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicI32;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicI32(&slice[index]);
                a_val
            },
        ),
        "i64" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicI64;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicI64(&slice[index]);
                a_val
            },
        ),
        "isize" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicIsize;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicIsize(&slice[index]);
                a_val
            },
        ),
        "u8" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicU8;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicU8(&slice[index]);
                a_val
            },
        ),
        "u16" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicU16;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicU16(&slice[index]);
                a_val
            },
        ),
        "u32" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicU32;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicU32(&slice[index]);
                a_val
            },
        ),
        "u64" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicU64;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicU64(&slice[index]);
                a_val
            },
        ),
        "usize" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicUsize;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicUsize(&slice[index]);
                a_val
            },
        ),
        "bool" => (
            quote! {
                let slice = unsafe {
                    let slice = self.data.__local_as_mut_slice();
                    let slice_ptr = slice.as_mut_ptr() as *mut std::sync::atomic::AtomicBool;
                    std::slice::from_raw_parts_mut(slice_ptr,slice.len())
                };
            },
            quote! {
                let mut a_val = #lamellar::array::native_atomic::MyAtomicBool(&slice[index]);
                a_val
            },
        ),
        _ => panic!("this should never happen {:?}", ident.to_string().as_str()),
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
        type_to_string(&typeident)
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
        let (_slice, val) = native_atomic_slice(&typeident, &lamellar);
        (
            quote! { #val },                                    //lhs
            quote! {slice[index].store(val, Ordering::SeqCst)}, //assign
            quote! {
                // println!("old value: {:?}, index: {:?}",slice[index].load(Ordering::SeqCst),index);
                res.push(slice[index].fetch_add(val, Ordering::SeqCst));
            }, //fetch_add
            quote! {
                res.push(slice[index].fetch_sub(val, Ordering::SeqCst));
            }, //fetch_sub
            quote! { //fetch_mul
                let mut old = slice[index].load(Ordering::SeqCst);
                let mut new = old * val;
                while slice[index].compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                    new = old * val;
                }
                res.push(old);
            },
            quote! { //fetch_div
                let mut old = slice[index].load(Ordering::SeqCst);
                let mut new = old / val;
                while slice[index].compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                    new = old / val;
                }
                res.push(old);
            },
            quote! { //fetch_rem
                let mut old = slice[index].load(Ordering::SeqCst);
                let mut new = old % val;
                while slice[index].compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                    new = old % val;
                }
                res.push(old);
            },
            quote! {
                res.push(slice[index].fetch_and(val, Ordering::SeqCst));
            }, //fetch_and
            quote! {
                res.push(slice[index].fetch_or(val, Ordering::SeqCst));
            }, //fetch_or
            quote! {
                res.push(slice[index].fetch_xor(val, Ordering::SeqCst));
            }, //fetch_xor
            quote! {slice[index].load(Ordering::SeqCst)}, //load
            quote! { //swap
                let mut old = slice[index].load(Ordering::SeqCst);
                while slice[index].compare_exchange(old, val, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                }
                res.push(old);
            },
            quote! { //compare_exchange
                let t_res = slice[index].compare_exchange(old, val, Ordering::SeqCst, Ordering::SeqCst);
                res.push(t_res);
            },
            quote! { //compare exchange epsilon
                let t_res = match slice[index].compare_exchange(old, val, Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(orig) => { //woohoo dont need to do worry about the epsilon
                        Ok(val)
                    },
                    Err(orig) => { //we dont match exactly, so we need to do the epsilon check
                        let mut done = false;
                        let mut orig = orig;
                        while (orig.abs_diff(old) as #typeident) < eps && !done{ //keep trying while under epsilon
                            orig = match slice[index].compare_exchange(orig, val, Ordering::SeqCst, Ordering::SeqCst) {
                                Ok(old_val) => { //we did it!
                                    done = true;
                                    old_val
                                },
                                Err(old_val) => { //someone else exchanged first!
                                    old_val
                                },
                            }
                        }
                        if done{
                            Ok(orig)
                        }
                        else{
                            Err(orig)
                        }
                    },
                };
                res.push(t_res);
            },
            quote! { //shl
                let mut old = slice[index].load(Ordering::SeqCst);
                let mut new = old << val;
                while slice[index].compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                    new = old << val;
                }
            },
            quote! { //fetch_shl
                let mut old = slice[index].load(Ordering::SeqCst);
                let mut new = old << val;
                while slice[index].compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                    new = old << val;
                }
                res.push(old);
            },
            quote! { //shr
                let mut old = slice[index].load(Ordering::SeqCst);
                let mut new = old >> val;
                while slice[index].compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                    new = old >> val;
                }
            },
            quote! { //fetch_shr
                let mut old = slice[index].load(Ordering::SeqCst);
                let mut new = old >> val;
                while slice[index].compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                    new = old >> val;
                }
                res.push(old);
            },
        )
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
        let (slice, _val) = native_atomic_slice(&typeident, &lamellar);
        (
            quote! {}, //no lock since its native atomic
            quote! { #slice },
        )
    } else if array_type == "LocalLockArray" {
        (
            quote! {}, //no explicit lock since the slice handle is a lock guard
            quote! {let mut slice = self.data.write_local_data().await; }, //this is the lock
        )
    } else if array_type == "GlobalLockArray" {
        (
            quote! {}, //no explicit lock since the slice handle is a lock guard
            quote! {let mut slice = self.data.write_local_data().await;}, //this is the lock
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
            #[#am_data(Debug,AmGroup(false))]
            struct #multi_val_multi_idx_am_buf_name{
                data: #lamellar::array::#array_type<#typeident>,
                op: #lamellar::array::ArrayOpCmd<#typeident>,
                #[serde(with = #serde_bytes)]
                idx_vals: Vec<u8>,
                index_size: u8,
            }
            #[#am(AmGroup(false))]
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
            #[#am_data(Debug,AmGroup(false))]
            struct #single_val_multi_idx_am_buf_name{
                data: #lamellar::array::#array_type<#typeident>,
                op: #lamellar::array::ArrayOpCmd<#typeident>,
                val: #typeident,
                #[serde(with = #serde_bytes)]
                indices: Vec<u8>,
                index_size: u8,
            }
            #[#am(AmGroup(false))]
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
            #[#am_data(Debug,AmGroup(false))]
            struct #multi_val_single_idx_am_buf_name{
                data: #lamellar::array::#array_type<#typeident>,
                op: #lamellar::array::ArrayOpCmd<#typeident>,
                #[serde(with = #serde_bytes)]
                vals: Vec<u8>,
                index: usize,
            }
            #[#am(AmGroup(false))]
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
                #[#am_data(Debug,AmGroup(false))]
                struct #multi_val_multi_idx_am_buf_result_name{
                    data: #lamellar::array::#array_type<#typeident>,
                    op: #lamellar::array::ArrayOpCmd<#typeident>,
                    #[serde(with = #serde_bytes)]
                    idx_vals: Vec<u8>,
                    index_size: u8,
                }
                #[#am(AmGroup(false))]
                impl LamellarAM for #multi_val_multi_idx_am_buf_result_name{ //eventually we can return fetchs here too...
                    async fn exec(&self) -> Vec<Result<#typeident,#typeident>> {
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
                        res
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
                #[#am_data(Debug,AmGroup(false))]
                struct #single_val_multi_idx_am_buf_result_name{
                    data: #lamellar::array::#array_type<#typeident>,
                    op: #lamellar::array::ArrayOpCmd<#typeident>,
                    val: #typeident,
                    #[serde(with = #serde_bytes)]
                    indices: Vec<u8>,
                    index_size: u8,
                }
                #[#am(AmGroup(false))]
                impl LamellarAM for #single_val_multi_idx_am_buf_result_name{ //eventually we can return fetchs here too...
                    async fn exec(&self) -> Vec<Result<#typeident,#typeident>> {
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
                        res
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
                #[#am_data(Debug,AmGroup(false))]
                struct #multi_val_single_idx_am_buf_result_name{
                    data: #lamellar::array::#array_type<#typeident>,
                    op: #lamellar::array::ArrayOpCmd<#typeident>,
                    #[serde(with = #serde_bytes)]
                    vals: Vec<u8>,
                    index: usize,
                }
                #[#am(AmGroup(false))]
                impl LamellarAM for #multi_val_single_idx_am_buf_result_name{ //eventually we can return fetchs here too...
                    async fn exec(&self) -> Vec<Result<#typeident,#typeident>>  {
                        // println!("in multi val single idx result exec");
                        #slice
                        let vals = unsafe {std::slice::from_raw_parts(self.vals.as_ptr() as *const #typeident, self.vals.len()/std::mem::size_of::<#typeident>())};
                        let index = self.index;
                        let mut res = Vec::new();
                        match self.op {
                            #multi_val_single_idx_result_match_stmts
                        }
                        res
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
        #[#am_data(Debug,AmGroup(false))]
        struct #multi_val_multi_idx_am_buf_fetch_name{
            data: #lamellar::array::#array_type<#typeident>,
            op: #lamellar::array::ArrayOpCmd<#typeident>,
            #[serde(with = #serde_bytes)]
            idx_vals: Vec<u8>,
            index_size: u8,
        }
        #[#am(AmGroup(false))]
        impl LamellarAM for #multi_val_multi_idx_am_buf_fetch_name{ //eventually we can return fetchs here too...
            async fn exec(&self) -> Vec<#typeident> {
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
                res
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
        #[#am_data(Debug,AmGroup(false))]
        struct #single_val_multi_idx_am_buf_fetch_name{
            data: #lamellar::array::#array_type<#typeident>,
            op: #lamellar::array::ArrayOpCmd<#typeident>,
            val: #typeident,
            #[serde(with = #serde_bytes)]
            indices: Vec<u8>,
            index_size: u8,
        }
        #[#am(AmGroup(false))]
        impl LamellarAM for #single_val_multi_idx_am_buf_fetch_name{ //eventually we can return fetchs here too...
            async fn exec(&self) -> Vec<#typeident>{
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
                res
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
        #[#am_data(Debug,AmGroup(false))]
        struct #multi_val_single_idx_am_buf_fetch_name{
            data: #lamellar::array::#array_type<#typeident>,
            op: #lamellar::array::ArrayOpCmd<#typeident>,
            #[serde(with = #serde_bytes)]
            vals: Vec<u8>,
            index: usize,
        }
        #[#am(AmGroup(false))]
        impl LamellarAM for #multi_val_single_idx_am_buf_fetch_name{ //eventually we can return fetchs here too...
            async fn exec(&self) -> Vec<#typeident> {
                // println!("in multi val single idx fetch exec");
                #slice
                let vals = unsafe {std::slice::from_raw_parts(self.vals.as_ptr() as *const #typeident, self.vals.len()/std::mem::size_of::<#typeident>())};
                let index = self.index;
                let mut res = Vec::new();
                match self.op {
                    #multi_val_single_idx_fetch_match_stmts
                }
                // println!("res: {:?}",res);
                res
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
    let mut atomic_array_types: Vec<(syn::Ident, syn::Ident, syn::Ident)> = vec![
        (
            quote::format_ident!("LocalLockArray"),
            quote::format_ident!("LocalLockByteArrayWeak"),
            quote::format_ident!("LocalLockByteArray"),
        ),
        (
            quote::format_ident!("GlobalLockArray"),
            quote::format_ident!("GlobalLockByteArrayWeak"),
            quote::format_ident!("GlobalLockByteArray"),
        ),
    ];

    if native {
        atomic_array_types.push((
            quote::format_ident!("NativeAtomicArray"),
            quote::format_ident!("NativeAtomicByteArrayWeak"),
            quote::format_ident!("NativeAtomicByteArray"),
        ));
    } else {
        atomic_array_types.push((
            quote::format_ident!("GenericAtomicArray"),
            quote::format_ident!("GenericAtomicByteArrayWeak"),
            quote::format_ident!("GenericAtomicByteArray"),
        ));
    }

    let mut expanded = quote! {};

    let ro_optypes = vec![OpType::ReadOnly]; //, vec![OpType::Arithmetic, OpType::Access];

    let buf_op_impl = create_buf_ops(
        typeident.clone(),
        quote::format_ident!("ReadOnlyArray"),
        quote::format_ident!("ReadOnlyByteArray"),
        &ro_optypes,
        rt,
    );
    expanded.extend(buf_op_impl);

    let buf_op_impl = create_buf_ops(
        typeident.clone(),
        quote::format_ident!("UnsafeArray"),
        quote::format_ident!("UnsafeByteArray"),
        &optypes,
        rt,
    );
    expanded.extend(buf_op_impl);

    for (array_type, _byte_array_type_weak, byte_array_type) in atomic_array_types {
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

fn test_ops(typeident: syn::Type, op_types: Vec<OpType>) -> proc_macro2::TokenStream {
    let lamellar = quote::format_ident!("crate");
    let (am_data, am): (syn::Path, syn::Path) = 
        (
            syn::parse("lamellar_impl::AmDataRT".parse().unwrap()).unwrap(),
            syn::parse("lamellar_impl::rt_am".parse().unwrap()).unwrap(),
        );
    let multi_val_multi_idx_name = quote::format_ident!(
        "MultiValMultiIdxAm{}",type_to_string(&typeident)
    );
    let multi_val_multi_idx_fetch_name = quote::format_ident!(
        "MultiValMultiIdxFetchAm{}",type_to_string(&typeident)
    );
    let multi_val_multi_idx_result_name = quote::format_ident!(
        "MultiValMultiIdxResultAm{}",type_to_string(&typeident)
    );
    let create_multi_val_multi_idx_name = quote::format_ident!(
        "CreateMultiValMultiIdxAm{}",type_to_string(&typeident)
    );
    let multi_val_multi_idx_id_new_name = quote::format_ident!(
        "MultiValMultiIdxId{}",type_to_string(&typeident)
    );
    let single_val_multi_idx_name = quote::format_ident!(
        "SingleValMultiIdxAm{}",type_to_string(&typeident)
    );
    let single_val_multi_idx_fetch_name = quote::format_ident!(
        "SingleValMultiIdxFetchAm{}",type_to_string(&typeident)
    );
    let single_val_multi_idx_result_name = quote::format_ident!(
        "SingleValMultiIdxResultAm{}",type_to_string(&typeident)
    );
    let create_single_val_multi_idx_name = quote::format_ident!(
        "CreateSingleValMultiIdxAm{}",type_to_string(&typeident)
    );
    let single_val_multi_idx_id_new_name = quote::format_ident!(
        "SingleValMultiIdxId{}",type_to_string(&typeident)
    );
    let multi_val_single_idx_name = quote::format_ident!(
        "MultiValSingleIdxAm{}",type_to_string(&typeident)
    );
    let multi_val_single_idx_fetch_name = quote::format_ident!(
        "MultiValSingleIdxFetchAm{}",type_to_string(&typeident)
    );
    let multi_val_single_idx_result_name = quote::format_ident!(
        "MultiValSingleIdxResultAm{}",type_to_string(&typeident)
    );
    let create_multi_val_single_idx_name = quote::format_ident!(
        "CreateMultiValSingleIdxAm{}",type_to_string(&typeident)
    );
    let multi_val_single_idx_id_new_name = quote::format_ident!(
        "MultiValSingleIdxId{}",type_to_string(&typeident)
    );

    let single_val_multi_idx_idx_vals = quote::quote! {
        let idx_vals = unsafe{ match self.index_size {
            1 => {
                Box::new(self.idxs.iter()
                    .map(|&idx| (idx as usize, self.val))) as Box<dyn Iterator<Item = (usize, #typeident)>>
            }
            2 => {
                Box::new(std::slice::from_raw_parts(self.idxs.as_ptr() as *const u16, self.idxs.len()/2).iter()
                    .map(|&idx| (idx as usize, self.val))) as Box<dyn Iterator<Item = (usize, #typeident)>>
            }
            4 => {
                Box::new(std::slice::from_raw_parts(self.idxs.as_ptr() as *const u32, self.idxs.len()/4).iter()
                    .map(|&idx| (idx as usize, self.val))) as Box<dyn Iterator<Item = (usize, #typeident)>>
            }
            8 => {
                Box::new(std::slice::from_raw_parts(self.idxs.as_ptr() as *const u64, self.idxs.len()/8).iter()
                    .map(|&idx| (idx as usize, self.val))) as Box<dyn Iterator<Item = (usize, #typeident)>>
            }
            _ => {
                Box::new(std::slice::from_raw_parts(self.idxs.as_ptr() as *const usize, self.idxs.len()/std::mem::size_of::<usize>()).iter()
                    .map(|&idx| (idx as usize, self.val))) as Box<dyn Iterator<Item = (usize, #typeident)>>
            }
        }};
    };

    let mut ops_mv_mi = quote!{};
    let mut fetch_ops_mv_mi = quote!{};
    let mut result_ops_mv_mi = quote!{};
    let mut ops_sv_mi = quote!{};
    let mut fetch_ops_sv_mi = quote!{};
    let mut result_ops_sv_mi = quote!{};
    let mut ops_mv_si = quote!{};
    let mut fetch_ops_mv_si = quote!{};
    let mut result_ops_mv_si = quote!{};
    let mv_mi_idx_vals = quote!{let idx_vals = IdxVal::<u8, #typeident>::iter_from_bytes(self.index_size as usize, &self.idxs_vals)};
    let sv_mi_idx_vals = quote!{#single_val_multi_idx_idx_vals};
    let mv_si_idx_vals = quote!{let idx_vals = std::iter::repeat(self.idx).zip(self.vals.iter().copied())};
    let local_data = quote!{let local_data = data.local_data::<#typeident>().await};
    let mut_local_data = quote!{let mut local_data = data.mut_local_data::<#typeident>().await};
    for op in op_types{
        match op{
            OpType::ReadOnly =>{
                
                fetch_ops_mv_mi.extend(quote!{
                    ArrayOpCmd::Load =>  {#local_data; #mv_mi_idx_vals; local_data.local_load(idx_vals)},
                });
                fetch_ops_sv_mi.extend(quote!{
                    ArrayOpCmd::Load =>  {#local_data; #sv_mi_idx_vals;local_data.local_load(idx_vals)},
                });
                fetch_ops_mv_si.extend(quote!{
                    ArrayOpCmd::Load =>  {#local_data; #mv_si_idx_vals;local_data.local_load(idx_vals)},
                });
            }
            OpType::Access => {
                ops_mv_mi.extend(quote!{
                    ArrayOpCmd::Store => {#mut_local_data; #mv_mi_idx_vals; local_data.local_store(idx_vals)},
                });
                ops_sv_mi.extend(quote!{
                    ArrayOpCmd::Store => {#mut_local_data; #sv_mi_idx_vals; local_data.local_store(idx_vals)},
                });
                ops_mv_si.extend(quote!{
                    ArrayOpCmd::Store => {#mut_local_data; #mv_si_idx_vals; local_data.local_store(idx_vals)},
                });
                fetch_ops_mv_mi.extend(quote!{
                    ArrayOpCmd::Swap =>  {#mut_local_data; #mv_mi_idx_vals; local_data.local_swap(idx_vals)},
                    
                });
                fetch_ops_sv_mi.extend(quote!{
                    ArrayOpCmd::Swap =>  {#mut_local_data; #sv_mi_idx_vals; local_data.local_swap(idx_vals)},
                    
                });
                fetch_ops_mv_si.extend(quote!{
                    ArrayOpCmd::Swap =>  {#mut_local_data; #mv_si_idx_vals; local_data.local_swap(idx_vals)},
                    
                });
            }
            OpType::Arithmetic => {
                ops_mv_mi.extend(quote!{
                    ArrayOpCmd::Add => {#mut_local_data; #mv_mi_idx_vals; local_data.local_add(idx_vals)},
                    ArrayOpCmd::Sub => {#mut_local_data; #mv_mi_idx_vals; local_data.local_sub(idx_vals)},
                    ArrayOpCmd::Mul => {#mut_local_data; #mv_mi_idx_vals; local_data.local_mul(idx_vals)},
                    ArrayOpCmd::Div => {#mut_local_data; #mv_mi_idx_vals; local_data.local_div(idx_vals)},
                    ArrayOpCmd::Rem => {#mut_local_data; #mv_mi_idx_vals; local_data.local_rem(idx_vals)},
                });
                ops_sv_mi.extend(quote!{
                    ArrayOpCmd::Add => {#mut_local_data; #sv_mi_idx_vals; local_data.local_add(idx_vals)},
                    ArrayOpCmd::Sub => {#mut_local_data; #sv_mi_idx_vals; local_data.local_sub(idx_vals)},
                    ArrayOpCmd::Mul => {#mut_local_data; #sv_mi_idx_vals; local_data.local_mul(idx_vals)},
                    ArrayOpCmd::Div => {#mut_local_data; #sv_mi_idx_vals; local_data.local_div(idx_vals)},
                    ArrayOpCmd::Rem => {#mut_local_data; #sv_mi_idx_vals; local_data.local_rem(idx_vals)},
                });
                ops_mv_si.extend(quote!{
                    ArrayOpCmd::Add => {#mut_local_data; #mv_si_idx_vals; local_data.local_add(idx_vals)},
                    ArrayOpCmd::Sub => {#mut_local_data; #mv_si_idx_vals; local_data.local_sub(idx_vals)},
                    ArrayOpCmd::Mul => {#mut_local_data; #mv_si_idx_vals; local_data.local_mul(idx_vals)},
                    ArrayOpCmd::Div => {#mut_local_data; #mv_si_idx_vals; local_data.local_div(idx_vals)},
                    ArrayOpCmd::Rem => {#mut_local_data; #mv_si_idx_vals; local_data.local_rem(idx_vals)},
                });
                fetch_ops_mv_mi.extend(quote!{
                    ArrayOpCmd::FetchAdd => {#mut_local_data; #mv_mi_idx_vals; local_data.local_fetch_add(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchSub => {#mut_local_data; #mv_mi_idx_vals; local_data.local_fetch_sub(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchMul => {#mut_local_data; #mv_mi_idx_vals; local_data.local_fetch_mul(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchDiv => {#mut_local_data; #mv_mi_idx_vals; local_data.local_fetch_div(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchRem => {#mut_local_data; #mv_mi_idx_vals; local_data.local_fetch_rem(idx_vals, true).unwrap()},
                });
                fetch_ops_sv_mi.extend(quote!{
                    ArrayOpCmd::FetchAdd => {#mut_local_data; #sv_mi_idx_vals; local_data.local_fetch_add(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchSub => {#mut_local_data; #sv_mi_idx_vals; local_data.local_fetch_sub(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchMul => {#mut_local_data; #sv_mi_idx_vals; local_data.local_fetch_mul(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchDiv => {#mut_local_data; #sv_mi_idx_vals; local_data.local_fetch_div(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchRem => {#mut_local_data; #sv_mi_idx_vals; local_data.local_fetch_rem(idx_vals, true).unwrap()},
                });
                fetch_ops_mv_si.extend(quote!{
                    ArrayOpCmd::FetchAdd => {#mut_local_data; #mv_si_idx_vals; local_data.local_fetch_add(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchSub => {#mut_local_data; #mv_si_idx_vals; local_data.local_fetch_sub(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchMul => {#mut_local_data; #mv_si_idx_vals; local_data.local_fetch_mul(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchDiv => {#mut_local_data; #mv_si_idx_vals; local_data.local_fetch_div(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchRem => {#mut_local_data; #mv_si_idx_vals; local_data.local_fetch_rem(idx_vals, true).unwrap()},
                });
            }
            OpType::CompExEps=> {
                result_ops_mv_mi.extend(quote!{
                    ArrayOpCmd::CompareExchangeEps(cur,eps) => {#mut_local_data; #mv_mi_idx_vals; local_data.local_compare_exchange_epsilon(idx_vals, cur, eps)},
                });
                result_ops_sv_mi.extend(quote!{
                    ArrayOpCmd::CompareExchangeEps(cur,eps) => {#mut_local_data; #sv_mi_idx_vals; local_data.local_compare_exchange_epsilon(idx_vals, cur, eps)},
                });
                result_ops_mv_si.extend(quote!{
                    ArrayOpCmd::CompareExchangeEps(cur,eps) => {#mut_local_data; #mv_si_idx_vals; local_data.local_compare_exchange_epsilon(idx_vals, cur, eps)},
                });
            }
            OpType::Bitwise => {
                ops_mv_mi.extend(quote!{
                    ArrayOpCmd::And => {#mut_local_data; #mv_mi_idx_vals; local_data.local_bit_and(idx_vals)},
                    ArrayOpCmd::Or => {#mut_local_data; #mv_mi_idx_vals; local_data.local_bit_or(idx_vals)},
                    ArrayOpCmd::Xor => {#mut_local_data; #mv_mi_idx_vals; local_data.local_bit_xor(idx_vals)},
                });
                ops_sv_mi.extend(quote!{
                    ArrayOpCmd::And => {#mut_local_data; #sv_mi_idx_vals; local_data.local_bit_and(idx_vals)},
                    ArrayOpCmd::Or => {#mut_local_data; #sv_mi_idx_vals; local_data.local_bit_or(idx_vals)},
                    ArrayOpCmd::Xor => {#mut_local_data; #sv_mi_idx_vals; local_data.local_bit_xor(idx_vals)},
                });
                ops_mv_si.extend(quote!{
                    ArrayOpCmd::And => {#mut_local_data; #mv_si_idx_vals; local_data.local_bit_and(idx_vals)},
                    ArrayOpCmd::Or => {#mut_local_data; #mv_si_idx_vals; local_data.local_bit_or(idx_vals)},
                    ArrayOpCmd::Xor => {#mut_local_data; #mv_si_idx_vals; local_data.local_bit_xor(idx_vals)},
                });
                fetch_ops_mv_mi.extend(quote!{
                    ArrayOpCmd::FetchAnd => {#mut_local_data; #mv_mi_idx_vals; local_data.local_fetch_bit_and(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchOr => {#mut_local_data; #mv_mi_idx_vals; local_data.local_fetch_bit_or(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchXor => {#mut_local_data; #mv_mi_idx_vals; local_data.local_fetch_bit_xor(idx_vals, true).unwrap()},
                });
                fetch_ops_sv_mi.extend(quote!{
                    ArrayOpCmd::FetchAnd => {#mut_local_data; #sv_mi_idx_vals; local_data.local_fetch_bit_and(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchOr => {#mut_local_data; #sv_mi_idx_vals; local_data.local_fetch_bit_or(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchXor => {#mut_local_data; #sv_mi_idx_vals; local_data.local_fetch_bit_xor(idx_vals, true).unwrap()},
                });
                fetch_ops_mv_si.extend(quote!{
                    ArrayOpCmd::FetchAnd => {#mut_local_data; #mv_si_idx_vals; local_data.local_fetch_bit_and(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchOr => {#mut_local_data; #mv_si_idx_vals; local_data.local_fetch_bit_or(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchXor => {#mut_local_data; #mv_si_idx_vals; local_data.local_fetch_bit_xor(idx_vals, true).unwrap()},
                });
            }
            OpType::Shift => {
                ops_mv_mi.extend(quote!{
                    ArrayOpCmd::Shl => {#mut_local_data; #mv_mi_idx_vals; local_data.local_shl(idx_vals)},
                    ArrayOpCmd::Shr => {#mut_local_data; #mv_mi_idx_vals; local_data.local_shr(idx_vals)},
                });
                ops_sv_mi.extend(quote!{
                    ArrayOpCmd::Shl => {#mut_local_data; #sv_mi_idx_vals; local_data.local_shl(idx_vals)},
                    ArrayOpCmd::Shr => {#mut_local_data; #sv_mi_idx_vals; local_data.local_shr(idx_vals)},
                });
                ops_mv_si.extend(quote!{
                    ArrayOpCmd::Shl => {#mut_local_data; #mv_si_idx_vals; local_data.local_shl(idx_vals)},
                    ArrayOpCmd::Shr => {#mut_local_data; #mv_si_idx_vals; local_data.local_shr(idx_vals)},
                });
                fetch_ops_mv_mi.extend(quote!{
                    ArrayOpCmd::FetchShl => {#mut_local_data; #mv_mi_idx_vals; local_data.local_fetch_shl(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchShr => {#mut_local_data; #mv_mi_idx_vals; local_data.local_fetch_shr(idx_vals, true).unwrap()},
                });
                fetch_ops_sv_mi.extend(quote!{
                    ArrayOpCmd::FetchShl => {#mut_local_data; #sv_mi_idx_vals; local_data.local_fetch_shl(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchShr => {#mut_local_data; #sv_mi_idx_vals; local_data.local_fetch_shr(idx_vals, true).unwrap()},
                });
                fetch_ops_mv_si.extend(quote!{
                    ArrayOpCmd::FetchShl => {#mut_local_data; #mv_si_idx_vals; local_data.local_fetch_shl(idx_vals, true).unwrap()},
                    ArrayOpCmd::FetchShr => {#mut_local_data; #mv_si_idx_vals; local_data.local_fetch_shr(idx_vals, true).unwrap()},
                });
            }
            OpType::CompEx => {
                result_ops_mv_mi.extend(quote!{
                    ArrayOpCmd::CompareExchange(cur) => {#mut_local_data; #mv_mi_idx_vals; local_data.local_compare_exchange(idx_vals, cur)},
                });
                result_ops_sv_mi.extend(quote!{
                    ArrayOpCmd::CompareExchange(cur) => {#mut_local_data; #sv_mi_idx_vals; local_data.local_compare_exchange(idx_vals, cur)},
                });
                result_ops_mv_si.extend(quote!{
                    ArrayOpCmd::CompareExchange(cur) => {#mut_local_data; #mv_si_idx_vals; local_data.local_compare_exchange(idx_vals, cur)},
                });
            }
        }
    }
    
    quote! {
        #[allow(non_camel_case_types)]
        #[#am_data(AmGroup(false))]
        struct #multi_val_multi_idx_name{
            data: LamellarByteArray,
            op: ArrayOpCmd<#typeident>,
            idxs_vals: Vec<u8>,
            index_size: u8,
        }

        #[#am(AmGroup(false))]
        impl LamellarAm for #multi_val_multi_idx_name{
            async fn exec(&self) {
                let mut data = self.data.clone();
                // let mut local_data = data.mut_local_data::<#typeident>().await;
                // let idx_vals = IdxVal::<u8, #typeident>::iter_from_bytes(self.index_size as usize, &self.idxs_vals);
                match self.op {
                    #ops_mv_mi
                    _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdxAm")
                }
            }
        }
        #[allow(non_camel_case_types)]
        #[#am_data(AmGroup(false))]
        struct #multi_val_multi_idx_fetch_name{
            data: LamellarByteArray,
            op: ArrayOpCmd<#typeident>,
            idxs_vals: Vec<u8>,
            index_size: u8,
        }

        #[#am(AmGroup(false))]
        impl LamellarAm for #multi_val_multi_idx_fetch_name{
            async fn exec(&self) -> Vec<#typeident> {
                let mut data = self.data.clone();
                // let mut local_data = data.mut_local_data::<#typeident>().await;
                // let idx_vals = IdxVal::<u8, #typeident>::iter_from_bytes(self.index_size as usize, &self.idxs_vals);
                match self.op {
                    #fetch_ops_mv_mi
                    _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdxAm")
                }
            }
        }
        #[allow(non_camel_case_types)]
        #[#am_data(AmGroup(false))]
        struct #multi_val_multi_idx_result_name{
            data: LamellarByteArray,
            op: ArrayOpCmd<#typeident>,
            idxs_vals: Vec<u8>,
            index_size: u8,
        }

        #[#am(AmGroup(false))]
        impl LamellarAm for #multi_val_multi_idx_result_name{
            async fn exec(&self) -> Vec<Result<#typeident, #typeident>> {
                let mut data = self.data.clone();
                // let mut local_data = data.mut_local_data::<#typeident>().await;
                // let idx_vals = IdxVal::<u8, #typeident>::iter_from_bytes(self.index_size as usize, &self.idxs_vals);
                match self.op {
                    #result_ops_mv_mi
                    _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdxAm")
                }
            }
        }
        fn #create_multi_val_multi_idx_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, idx_vals: Vec<u8>, index_size: u8, return_type: #lamellar::array::BatchReturnType) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
            match return_type {
                #lamellar::array::BatchReturnType::None => {
                    Arc::new(#multi_val_multi_idx_name{
                        data: Into::into(array),
                        op: op.into(),
                        idxs_vals: idx_vals,
                        index_size,
                    })
                }
                #lamellar::array::BatchReturnType::Vals => {
                    Arc::new(#multi_val_multi_idx_fetch_name{
                        data: Into::into(array),
                        op: op.into(),
                        idxs_vals: idx_vals,
                        index_size,
                    })
                }
                #lamellar::array::BatchReturnType::Result => {
                    Arc::new(#multi_val_multi_idx_result_name{
                        data: Into::into(array),
                        op: op.into(),
                        idxs_vals: idx_vals,
                        index_size,
                    })
                }
            }
        }
        fn #multi_val_multi_idx_id_new_name() -> std::any::TypeId {
            std::any::TypeId::of::<#typeident>()
        }
        inventory::submit! {
            #lamellar::array::multi_val_multi_idx_ops_new{
                id: #multi_val_multi_idx_id_new_name,
                op: #create_multi_val_multi_idx_name,
            } 
        }

        #[lamellar_impl::AmDataRT(AmGroup(false))]
        struct #single_val_multi_idx_name{
            data: LamellarByteArray,
            op: ArrayOpCmd<#typeident>,
            val: #typeident,
            idxs: Vec<u8>,
            index_size: u8,
        }
        #[lamellar_impl::rt_am]
        impl LamellarAm for #single_val_multi_idx_name{
            async fn exec(&self) {
                let mut data = self.data.clone();
                // let mut local_data = data.mut_local_data::<#typeident>().await;
                // #single_val_multi_idx_idx_vals
                match self.op {
                    #ops_sv_mi
                    _ => panic!("Invalid op: {:#?}", self.op)
                }
            }
        }
        #[lamellar_impl::AmDataRT(AmGroup(false))]
        struct #single_val_multi_idx_fetch_name{
            data: LamellarByteArray,
            op: ArrayOpCmd<#typeident>,
            val: #typeident,
            idxs: Vec<u8>,
            index_size: u8,
        }
        #[lamellar_impl::rt_am]
        impl LamellarAm for #single_val_multi_idx_fetch_name{
            async fn exec(&self) -> Vec<#typeident> {
                let mut data = self.data.clone();
                // let mut local_data = data.mut_local_data::<#typeident>().await;
                // #single_val_multi_idx_idx_vals
                match self.op {
                    #fetch_ops_sv_mi
                    _ => panic!("Invalid op: {:#?}", self.op)
                }
            }
        }
        #[lamellar_impl::AmDataRT(AmGroup(false))]
        struct #single_val_multi_idx_result_name{
            data: LamellarByteArray,
            op: ArrayOpCmd<#typeident>,
            val: #typeident,
            idxs: Vec<u8>,
            index_size: u8,
        }
        #[lamellar_impl::rt_am]
        impl LamellarAm for #single_val_multi_idx_result_name{
            async fn exec(&self) -> Vec<Result<#typeident, #typeident>> {
                let mut data = self.data.clone();
                // let mut local_data = data.mut_local_data::<#typeident>().await;
                // #single_val_multi_idx_idx_vals
                match self.op {
                    #result_ops_sv_mi
                    _ => panic!("Invalid op: {:#?}", self.op)
                }
            }
        }
        fn #create_single_val_multi_idx_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, val_bytes: Vec<u8>, idxs: Vec<u8>, index_size: u8, return_type: #lamellar::array::BatchReturnType) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
            match return_type {
                #lamellar::array::BatchReturnType::None => {
                    Arc::new(#single_val_multi_idx_name{
                        data: Into::into(array),
                        op: op.into(),
                        val: unsafe{*(val_bytes.as_ptr() as *const #typeident)}, 
                        idxs: idxs,
                        index_size,
                    })
                }
                #lamellar::array::BatchReturnType::Vals => {
                    Arc::new(#single_val_multi_idx_fetch_name{
                        data: Into::into(array),
                        op: op.into(),
                        val: unsafe{*(val_bytes.as_ptr() as *const #typeident)}, 
                        idxs: idxs,
                        index_size,
                    })
                }
                #lamellar::array::BatchReturnType::Result => {
                    Arc::new(#single_val_multi_idx_result_name{
                        data: Into::into(array),
                        op: op.into(),
                        val: unsafe{*(val_bytes.as_ptr() as *const #typeident)}, 
                        idxs: idxs,
                        index_size,
                    })
                }
            }
        }
        fn #single_val_multi_idx_id_new_name() -> std::any::TypeId {
            std::any::TypeId::of::<#typeident>()
        }
        inventory::submit! {
            #lamellar::array::single_val_multi_idx_ops_new{
                id: #single_val_multi_idx_id_new_name,
                op: #create_single_val_multi_idx_name,
            }
        }

        #[allow(non_camel_case_types)]
        #[#am_data(AmGroup(false))]
        struct #multi_val_single_idx_name{
            data: LamellarByteArray,
            op: ArrayOpCmd<#typeident>,
            idx: usize,
            vals: Vec<#typeident>,
        }

        #[#am(AmGroup(false))]
        impl LamellarAm for #multi_val_single_idx_name{
            async fn exec(&self) {
                let mut data = self.data.clone();
                // let mut local_data = data.mut_local_data::<#typeident>().await;
                // let idx_vals = std::iter::repeat(self.idx).zip(self.vals.iter().copied());
                match self.op {
                    #ops_mv_si
                    _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdxAm")
                }
            }
        }
        #[allow(non_camel_case_types)]
        #[#am_data(AmGroup(false))]
        struct #multi_val_single_idx_fetch_name{
            data: LamellarByteArray,
            op: ArrayOpCmd<#typeident>,
            idx: usize,
            vals: Vec<#typeident>,
        }

        #[#am(AmGroup(false))]
        impl LamellarAm for #multi_val_single_idx_fetch_name{
            async fn exec(&self) -> Vec<#typeident> {
                let mut data = self.data.clone();
                // let mut local_data = data.mut_local_data::<#typeident>().await;
                // let idx_vals = std::iter::repeat(self.idx).zip(self.vals.iter().copied());
                match self.op {
                    #fetch_ops_mv_si
                    _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdxAm")
                }
            }
        }
        #[allow(non_camel_case_types)]
        #[#am_data(AmGroup(false))]
        struct #multi_val_single_idx_result_name{
            data: LamellarByteArray,
            op: ArrayOpCmd<#typeident>,
            idx: usize,
            vals: Vec<#typeident>,
        }

        #[#am(AmGroup(false))]
        impl LamellarAm for #multi_val_single_idx_result_name{
            async fn exec(&self) -> Vec<Result<#typeident, #typeident>> {
                let mut data = self.data.clone();
                // let mut local_data = data.mut_local_data::<#typeident>().await;
                // let idx_vals = std::iter::repeat(self.idx).zip(self.vals.iter().copied());
                match self.op {
                    #result_ops_mv_si
                    _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdxAm")
                }
            }
        }
        fn #create_multi_val_single_idx_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd<Vec<u8>>, vals: (*const u8,usize,usize), idx: usize, return_type: #lamellar::array::BatchReturnType) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
            let vals = unsafe {Vec::from_raw_parts(vals.0 as *mut #typeident,vals.1,vals.2)};
            match return_type {
                #lamellar::array::BatchReturnType::None => {
                    Arc::new(#multi_val_single_idx_name{
                        data: Into::into(array),
                        op: op.into(),
                        idx,
                        vals,
                    })
                }
                #lamellar::array::BatchReturnType::Vals => {
                    Arc::new(#multi_val_single_idx_fetch_name{
                        data: Into::into(array),
                        op: op.into(),
                        idx,
                        vals,
                    })
                }
                #lamellar::array::BatchReturnType::Result => {
                    Arc::new(#multi_val_single_idx_result_name{
                        data: Into::into(array),
                        op: op.into(),
                        idx,
                        vals,
                    })
                }
            }
        }
        fn #multi_val_single_idx_id_new_name() -> std::any::TypeId {
            std::any::TypeId::of::<#typeident>()
        }

        inventory::submit! {
            #lamellar::array::multi_val_single_idx_ops_new{
                id: #multi_val_single_idx_id_new_name,
                op: #create_multi_val_single_idx_name,
            }
        }
    }
}



pub(crate) fn __generate_ops_for_type_rt(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    let items = item
        .to_string()
        .split(",")
        .map(|i| i.to_owned())
        .collect::<Vec<String>>();
    let mut op_types = vec![
        OpType::ReadOnly,
        OpType::Access,
        OpType::Arithmetic,
        OpType::CompExEps,
    ];
    let mut opt_op_types = vec![OpType::ReadOnly, OpType::Access];
    let bitwise = if let Ok(val) = syn::parse_str::<syn::LitBool>(&items[0]) {
        if val.value {
            op_types.push(OpType::Bitwise);
            op_types.push(OpType::Shift);
        }
        // true
        val.value
    } else {
        panic! ("first argument of generate_ops_for_type expects 'true' or 'false' specifying whether type implements bitwise operations");
    };
    let native = if let Ok(val) = syn::parse_str::<syn::LitBool>(&items[1]) {
        val.value
    } else {
        panic! ("second argument of generate_ops_for_type expects 'true' or 'false' specifying whether types are native atomics");
    };

    let impl_eq = if let Ok(val) = syn::parse_str::<syn::LitBool>(&items[2]) {
        if val.value {
            op_types.push(OpType::CompEx);
            opt_op_types.push(OpType::CompEx);
        }
        val.value
    } else {
        panic! ("third argument of generate_ops_for_type expects 'true' or 'false' specifying whether types implement eq");
    };
    for t in items[3..].iter() {
        let the_type = syn::parse_str::<syn::Type>(&t).unwrap();
        let typeident = quote::format_ident!("{:}", t.trim());
        output.extend(quote! {
            impl Dist for #typeident {}
            // impl Dist for Option< #typeident > {}
            impl crate::array::operations::ArrayOps for #typeident {}
            // impl crate::array::operations::ArrayOps for Option< #typeident > {}
            impl ElementArithmeticOps for #typeident {}
            impl ElementComparePartialEqOps for #typeident {}
            // impl ElementComparePartialEqOps for Option< #typeident > {}
        });
        if bitwise {
            output.extend(quote! {
                impl ElementBitWiseOps for #typeident {}
                impl ElementShiftOps for #typeident {}
            });
        }
        if impl_eq {
            output.extend(quote! {
                impl ElementCompareEqOps for #typeident {}
                impl ElementCompareEqOps for Option< #typeident > {}
            })
        }
        // output.extend(create_buffered_ops(
        //     the_type.clone(),
        //     op_types.clone(),
        //     native,
        //     true,
        // ));
        let opt_type: syn::Type = syn::parse_str::<syn::Type>(&format!("Option<{}>", t)).unwrap();
        // output.extend(create_buffered_ops(
        //     opt_type.clone(),
        //     opt_op_types.clone(),
        //     false,
        //     true,
        // ));

        output.extend(test_ops(the_type.clone(),op_types.clone()));
        output.extend(test_ops(opt_type,opt_op_types.clone()));

        // output.extend(gen_atomic_rdma(typeident.clone(), true));
    }
    TokenStream::from(output)
}

pub(crate) fn __generate_ops_for_bool_rt() -> TokenStream {
    let mut output = quote! {};
    let op_types = vec![
        OpType::ReadOnly,
        OpType::Access,
        OpType::CompEx,
        OpType::Bitwise,
    ];

    // let bool_type = syn::Type::
    let the_type = syn::parse_str::<syn::Type>("bool").unwrap();
    output.extend(quote! {
        impl Dist for bool {}
        impl crate::array::operations::ArrayOps for bool {}
        impl ElementBitWiseOps for bool {}
        impl ElementCompareEqOps for bool {}
    });
    output.extend(create_buffered_ops(
        the_type.clone(),
        op_types.clone(),
        false,
        true,
    ));
    // output.extend(gen_atomic_rdma(typeident.clone(), true));
    TokenStream::from(output)
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
                ReadOnlyByteArray,
                UnsafeByteArray,
                LocalLockByteArray,
                GlobalLockByteArray,
                GenericAtomicByteArray,
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
