use proc_macro::TokenStream;
use proc_macro_error::abort;
use quote::{quote, quote_spanned, ToTokens};
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

fn create_buf_ops(
    typeident: syn::Type,
    array_type: syn::Ident,
    byte_array_type: syn::Ident,
    optypes: &Vec<OpType>,
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

    let res_t = quote! {
        let res_t = unsafe{std::slice::from_raw_parts_mut(results_u8.as_mut_ptr().offset(results_offset as isize) as *mut #typeident,1)};
        results_offset += std::mem::size_of::<#typeident>();
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
            quote! { #val },                                                           //lhs
            quote! {slice[index].store(val, Ordering::SeqCst)},                        //assign
            quote! {#res_t res_t[0] = slice[index].fetch_add(val, Ordering::SeqCst);}, //fetch_add
            quote! {#res_t res_t[0] = slice[index].fetch_sub(val, Ordering::SeqCst);}, //fetch_sub
            quote! { //fetch_mul
                let mut old = slice[index].load(Ordering::SeqCst);
                let mut new = old * val;
                while slice[index].compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                    new = old * val;
                }
                #res_t res_t[0] = old;
            },
            quote! { //fetch_div
                let mut old = slice[index].load(Ordering::SeqCst);
                let mut new = old / val;
                while slice[index].compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                    new = old / val;
                }
                #res_t res_t[0] = old;
            },
            quote! { //fetch_rem
                let mut old = slice[index].load(Ordering::SeqCst);
                let mut new = old % val;
                while slice[index].compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                    new = old % val;
                }
                #res_t res_t[0] = old;
            },
            quote! {#res_t res_t[0] = slice[index].fetch_and(val, Ordering::SeqCst);}, //fetch_and
            quote! {#res_t res_t[0] = slice[index].fetch_or(val, Ordering::SeqCst);},  //fetch_or
            quote! {#res_t res_t[0] = slice[index].fetch_xor(val, Ordering::SeqCst);}, //fetch_or
            quote! {slice[index].load(Ordering::SeqCst)},                              //load
            quote! { //swap
                let mut old = slice[index].load(Ordering::SeqCst);
                while slice[index].compare_exchange(old, val, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = slice[index].load(Ordering::SeqCst);
                }
                #res_t res_t[0] = old;
            },
            quote! { //compare_exchange
                let old = match slice[index].compare_exchange(old, val, Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(old) => {
                        results_u8[results_offset] = 0;
                        results_offset+=1;
                        old
                    },
                    Err(old) => {
                        results_u8[results_offset] = 1;
                        results_offset+=1;
                        old
                    },
                };
                #res_t res_t[0] = old;
            },
            quote! { //compare exchange epsilon
                let old = match slice[index].compare_exchange(old, val, Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(orig) => { //woohoo dont need to do worry about the epsilon
                        results_u8[results_offset] = 0;
                        results_offset+=1;
                        val
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
                            results_u8[results_offset] = 0;
                            results_offset+=1;
                        }
                        else{
                            results_u8[results_offset] = 1;
                            results_offset+=1;
                        }
                        orig
                    },
                };
                #res_t res_t[0] = old;
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
                #res_t res_t[0] = old;
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
                #res_t res_t[0] = old;
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
            quote! {slice[index]},                                           //lhs
            quote! {slice[index] = val},                                     //assign
            quote! {#res_t res_t[0] =  slice[index]; slice[index] += val; }, //fetch_add -- we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] -= val; }, //fetch_sub --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] *= val; }, //fetch_mul --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] /= val; }, //fetch_div --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] %= val; }, //fetch_rem --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] &= val; }, //fetch_and --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] |= val; }, //fetch_or --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] ^= val; }, //fetch_xor --we lock the index before this point so its actually atomic
            quote! {slice[index]},                                           //load
            quote! {#res_t res_t[0] =  slice[index]; slice[index] = val; }, //swap we lock the index before this point so its actually atomic
            quote! {  // compare_exchange -- we lock the index before this point so its actually atomic
                 let old = if old == slice[index]{
                    slice[index] = val;
                    results_u8[results_offset] = 0;
                    results_offset+=1;
                    old
                } else {
                    results_u8[results_offset] = 1;
                    results_offset+=1;
                    slice[index]
                };
                #res_t res_t[0] = old;
            },
            quote! { //compare exchange epsilon
                let same = if old > slice[index] {
                    old - slice[index] < eps
                }
                else{
                    slice[index] - old < eps
                };
                let old = if same {
                    slice[index] = val;
                    results_u8[results_offset] = 0;
                    results_offset+=1;
                    old
                } else {
                    results_u8[results_offset] = 1;
                    results_offset+=1;
                    slice[index]
                };
                #res_t res_t[0] = old;
            },
            quote! { slice[index] <<= val; }, //shl --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] <<= val; }, //fetch_shl --we lock the index before this point so its actually atomic
            quote! { slice[index] >>= val; }, //shr --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] >>= val; }, //fetch_shr --we lock the index before this point so its actually atomic
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
            quote! {let mut slice = self.data.write_local_data();}, //this is the lock
        )
    } else if array_type == "GlobalLockArray" {
        (
            quote! {}, //no explicit lock since the slice handle is a lock guard
            quote! {let mut slice = self.data.async_write_local_data().await;}, //this is the lock
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

    let mut match_stmts = quote! {};
    for optype in optypes {
        match optype {
            OpType::Arithmetic => match_stmts.extend(quote! {
                ArrayOpCmd::Add=>{ #lhs += val },
                ArrayOpCmd::FetchAdd=> {
                    #fetch_add
                },
                ArrayOpCmd::Sub=>{#lhs -= val},
                ArrayOpCmd::FetchSub=>{
                    #fetch_sub
                },
                ArrayOpCmd::Mul=>{ #lhs *= val},
                ArrayOpCmd::FetchMul=>{
                    #fetch_mul
                },
                ArrayOpCmd::Div=>{ #lhs /= val },
                ArrayOpCmd::FetchDiv=>{
                    #fetch_div
                },
                ArrayOpCmd::Rem=>{ #lhs %= val },
                ArrayOpCmd::FetchRem=>{
                    #fetch_rem
                },
                ArrayOpCmd::Put => {#assign},
                ArrayOpCmd::Get =>{
                    #res_t res_t[0] = #load;

                }
            }),
            OpType::Bitwise => match_stmts.extend(quote! {
                ArrayOpCmd::And=>{#lhs &= val},
                ArrayOpCmd::FetchAnd=>{
                    #fetch_and
                },
                ArrayOpCmd::Or=>{#lhs |= val},
                ArrayOpCmd::FetchOr=>{
                    #fetch_or
                },
                ArrayOpCmd::Xor=>{#lhs ^= val},
                ArrayOpCmd::FetchXor=>{
                    #fetch_xor
                },
            }),
            OpType::Access => match_stmts.extend(quote! {
                ArrayOpCmd::Store=>{
                    #assign
                },
                ArrayOpCmd::Swap=>{
                    #swap
                },
            }),
            OpType::CompEx => match_stmts.extend(quote! {
                ArrayOpCmd::CompareExchange(old) =>{
                    #compare_exchange
                }
            }),
            OpType::CompExEps => match_stmts.extend(quote! {
                ArrayOpCmd::CompareExchangeEps(old,eps) =>{
                    #compare_exchange_eps
                }
            }),
            OpType::ReadOnly => match_stmts.extend(quote! {
                ArrayOpCmd::Load=>{
                    #res_t res_t[0] =  #load;
                },
            }),
            OpType::Shift => match_stmts.extend(quote! {
                ArrayOpCmd::Shl=>{#shl},
                ArrayOpCmd::FetchShl=>{
                    #fetch_shl
                },
                ArrayOpCmd::Shr=>{#shr},
                ArrayOpCmd::FetchShr=>{
                    #fetch_shr
                },
            }),
        }
    }

    let buf_op_name = quote::format_ident!("{}_{}_op_buf", array_type, type_to_string(&typeident));
    let am_buf_name = quote::format_ident!("{}_{}_am_buf", array_type, type_to_string(&typeident));
    let dist_am_buf_name =
        quote::format_ident!("{}_{}_am_buf", array_type, type_to_string(&typeident));
    let reg_name = quote::format_ident!("{}OpBuf", array_type);

    let inner_op = quote! {
        let index = *index;
        let val = *val;
        #lock //this will get dropped at end of loop
        let orig = #load;

        match op{
        # match_stmts
        _ => {panic!("shouldnt happen {:?}",op)}
        }
    };

    expanded.extend(quote! {
        struct #buf_op_name{
            data: #lamellar::array::#byte_array_type,
            ops: Mutex<Vec<(Vec<u8>,usize)>>,
            // new_ops: Mutex<Vec<(OneSidedMemoryRegion<u8>,usize)>>,
            cur_len: AtomicUsize, //this could probably just be a normal usize cause we only update it after we get ops lock
            complete: RwLock<Arc<AtomicBool>>,
            results_offset: RwLock<Arc<AtomicUsize>>,
            results:  RwLock<PeOpResults>,
        }
        #[#am_data(Debug)]
        struct #am_buf_name{
            data: #lamellar::array::#array_type<#typeident>,
            // ops: Vec<(ArrayOpCmd<#typeident>,#lamellar::array::OpAmInputToValue<#typeident>)>,
            ops: Vec<u8>,
            // new_ops: Vec<OneSidedMemoryRegion<u8>>,
            // ops2: OneSidedMemoryRegion<u8>,
            res_buf_size: usize,
            orig_pe: usize,
        }
        impl #lamellar::array::BufferOp for #buf_op_name{
            // #[#lamellar::tracing::instrument(skip_all)]
            fn add_ops(&self, op_ptr: *const u8, op_data_ptr: *const u8, team: Pin<Arc<LamellarTeamRT>>) -> (bool,Arc<AtomicBool>){
                // let span1 = #lamellar::tracing::trace_span!("convert");
                // let _guard = span1.enter();
                let op_data = unsafe{(&*(op_data_ptr as *const #lamellar::array::InputToValue<'_,#typeident>)).as_op_am_input()};
                let op = unsafe{*(op_ptr as *const ArrayOpCmd<#typeident>)};
                // drop(_guard);
                // let span2 = #lamellar::tracing::trace_span!("lock");
                // let _guard = span2.enter();
                // let mut bufs = self.new_ops.lock();
                let mut bufs = self.ops.lock();

                // let mut buf = self.ops.lock();

                // drop(_guard);
                // let span3 = #lamellar::tracing::trace_span!("update");
                // let _guard = span3.enter();
                let first = bufs.len() == 0;
                let op_size = op.num_bytes();
                let data_size = op_data.num_bytes();
                if first {
                    // bufs.push((team.alloc_one_sided_mem_region(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE,op_size+data_size)),0));
                    let mut v = Vec::with_capacity(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size));
                    unsafe {v.set_len(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size));}
                    bufs.push((v,0));
                }
                else {
                    if bufs.last().unwrap().1 + op_size+data_size > #lamellar::array::OPS_BUFFER_SIZE{
                        // bufs.push((team.alloc_one_sided_mem_region(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size)),0));
                        let mut v = Vec::with_capacity(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size));
                        unsafe {v.set_len(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size));}
                        bufs.push((v,0));
                    }
                }
                // let mut buf: &mut (OneSidedMemoryRegion<u8>, usize) = bufs.last_mut().unwrap();
                let mut buf: &mut (Vec<u8>, usize) = bufs.last_mut().unwrap();

                let _temp = self.cur_len.fetch_add(op_data.len(),Ordering::SeqCst);

                // let mut buf_slice = unsafe{buf.0.as_mut_slice().unwrap()};

                let mut buf_slice = buf.0.as_mut_slice();

                buf.1 += op.to_bytes(&mut buf_slice[(buf.1)..]);
                buf.1 += op_data.to_bytes(&mut buf_slice[(buf.1)..]);
                (first,self.complete.read().clone())
            }
            // #[#lamellar::tracing::instrument(skip_all)]
            fn add_fetch_ops(&self, pe: usize, op_ptr: *const u8, op_data_ptr: *const u8, req_ids: &Vec<usize>, res_map: OpResults, team: Pin<Arc<LamellarTeamRT>>) -> (bool,Arc<AtomicBool>,Option<OpResultOffsets>){
                let op_data = unsafe{(&*(op_data_ptr as *const #lamellar::array::InputToValue<'_,#typeident>)).as_op_am_input()};
                let op = unsafe{*(op_ptr as *const ArrayOpCmd<#typeident>)};
                let mut res_offsets = vec![];
                let mut bufs = self.ops.lock();
                // let mut bufs = self.new_ops.lock();

                for rid in req_ids{
                    let res_size = op.result_size();
                    res_offsets.push((*rid,self.results_offset.read().fetch_add(res_size,Ordering::SeqCst),res_size));
                }
                // let first = buf.len() == 0;
                // buf.push((op,op_data));
                let first = bufs.len() == 0;
                let op_size = op.num_bytes();
                let data_size = op_data.num_bytes();
                if first {
                    // bufs.push((team.alloc_one_sided_mem_region(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size)),0));
                    let mut v = Vec::with_capacity(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size));
                    unsafe {v.set_len(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size));}
                    bufs.push((v,0));
                }
                else {
                    if bufs.last().unwrap().1 + op_size+data_size > #lamellar::array::OPS_BUFFER_SIZE{
                        // bufs.push((team.alloc_one_sided_mem_region(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size)),0));
                        let mut v = Vec::with_capacity(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size));
                        unsafe {v.set_len(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size));}
                        bufs.push((v,0));
                    }
                }
                // let mut buf: &mut (OneSidedMemoryRegion<u8>, usize) = bufs.last_mut().unwrap();
                let mut buf: &mut (Vec<u8>, usize) = bufs.last_mut().unwrap();
                let _temp = self.cur_len.fetch_add(op_data.len(),Ordering::SeqCst);
                // let mut buf_slice = unsafe{buf.0.as_mut_slice().unwrap()};
                let mut buf_slice = buf.0.as_mut_slice();

                buf.1 += op.to_bytes(&mut buf_slice[(buf.1)..]);
                buf.1 += op_data.to_bytes(&mut buf_slice[(buf.1)..]);
                res_map.insert(pe,self.results.read().clone());
                (first,self.complete.read().clone(),Some(res_offsets))
            }
            // #[#lamellar::tracing::instrument(skip_all)]
            fn into_arc_am(&self, pe: usize, sub_array: std::ops::Range<usize>)-> (Vec<Arc<dyn RemoteActiveMessage + Sync + Send>>,usize,Arc<AtomicBool>, PeOpResults){
              
                let mut ams: Vec<Arc<dyn RemoteActiveMessage + Sync + Send>> = Vec::new();
                // let mut bufs = self.new_ops.lock();
                let mut bufs = self.ops.lock();

                let mut ops = Vec::new();
                let len = self.cur_len.load(Ordering::SeqCst);
                self.cur_len.store(0,Ordering::SeqCst);
                std::mem::swap(&mut ops,  &mut bufs);
                let mut complete = Arc::new(AtomicBool::new(false));
                std::mem::swap(&mut complete, &mut self.complete.write());
                let mut results = Arc::new(Mutex::new(Vec::new()));

                std::mem::swap(&mut results, &mut self.results.write());
                let mut result_buf_size = self.results_offset.read().swap(0,Ordering::SeqCst);

                let mut cur_size = 0;
                let mut op_i = ops.len() as isize-1;
                let data: #lamellar::array::#array_type<#typeident> = self.data.upgrade().expect("array invalid").into();
                for (mut lmr,size) in ops.drain(..){
                    unsafe { lmr.set_len(size);}
                    let mut am = #am_buf_name{
                        // wait: wait.clone(),
                        ops: lmr,
                        data: data.sub_array(sub_array.clone()),
                        // ops2: lmr.sub_region(0..size),
                        res_buf_size: result_buf_size,
                        orig_pe: data.my_pe(),
                    };
                    ams.push(Arc::new(am));
                }

                (ams,len,complete,results)
            }
        }

        impl #am_buf_name{
            // async fn get_ops(&self, team: &std::sync::Arc<#lamellar::LamellarTeam>) -> #lamellar::OneSidedMemoryRegion<u8>{
            //     unsafe{
            //         let serialized_ops = if self.ops2.data_local(){
            //             self.ops2.clone()
            //         }
            //         else{
            //             let serialized_ops = team.alloc_one_sided_mem_region::<u8>(self.ops2.len());
            //             let local_slice = serialized_ops.as_mut_slice().unwrap();
            //             local_slice[self.ops2.len()- 1] = 255u8;
            //             // self.ops2.get_unchecked(0, serialized_ops.clone());
            //             self.ops2.iget(0, serialized_ops.clone());

            //             while local_slice[self.ops2.len()- 1] == 255u8 {
            //                 // async_std::task::yield_now().await;
            //                 std::thread::yield_now();
            //             }
            //             serialized_ops
            //         };
            //         serialized_ops
            //         // self.ops2.iget(0,serialized_ops.clone());
            //         // #lamellar::deserialize(serialized_ops.as_mut_slice().unwrap(),false).unwrap()
            //     }
            // }

            fn get_op<'a>(&self, buf: &'a [u8]) -> (usize,(ArrayOpCmd<#typeident>,RemoteOpAmInputToValue<'a,#typeident>)){
                let mut bytes_read = 0;
                let (cmd,size) = ArrayOpCmd::from_bytes(buf);
                bytes_read += size;
                let (data,size) = RemoteOpAmInputToValue::from_bytes(& buf[bytes_read..]);
                bytes_read += size;
                (bytes_read,(cmd,data))
            }
        }
        #[#am]
        impl LamellarAM for #am_buf_name{ //eventually we can return fetchs here too...
            async fn exec(&self) -> Vec<u8>{
                // let timer=std::time::Instant::now();
                #slice
                let u8_len = self.res_buf_size;
                let mut results_u8: Vec<u8> = if u8_len > 0 {
                    let mut temp = Vec::with_capacity(u8_len);
                    unsafe{temp.set_len(u8_len)};
                    temp
                }
                else{
                    vec![]
                };
                let mut results_offset=0;
                // let mut results_slice = unsafe{std::slice::from_raw_parts_mut(results_u8.as_mut_ptr() as *mut #typeident,self.num_fetch_ops)};
                // let local_ops = self.get_ops(&lamellar::team).await;
                // let local_ops_slice = local_ops.as_slice().unwrap();
                let local_ops_slice = &self.ops;
                let mut cur_index = 0;
                while cur_index <  local_ops_slice.len(){
                    let (bytes_read,(op,ops)) = self.get_op(&local_ops_slice[cur_index..]);
                    cur_index += bytes_read;
                    match ops{
                        RemoteOpAmInputToValue::OneToOne(index,val) => {
                            // let index = *index;
                            // let val = *val;
                            #inner_op
                        },
                        RemoteOpAmInputToValue::OneToMany(index,vals) => {
                            for val in vals{ //there maybe an optimization here where we grab the index lock outside of the loop
                                #inner_op
                            }
                        },
                        RemoteOpAmInputToValue::ManyToOne(indices,val) => {
                            for index in indices{
                                #inner_op
                            }
                        },
                        RemoteOpAmInputToValue::ManyToMany(indices,vals) => {
                            for (index,val) in indices.iter().zip(vals.iter()){
                                #inner_op
                            }
                        },
                    }
                }
                // membarrier::heavy();
                // for (op, ops) in &local_ops { //(ArrayOpCmd,OpAmInputToValue)
                // for (op, ops) in &self.ops { //(ArrayOpCmd,OpAmInputToValue)
                //     match ops{
                //         OpAmInputToValue::OneToOne(index,val) => {
                //             #inner_op
                //         },
                //         OpAmInputToValue::OneToMany(index,vals) => {
                //             for val in vals{ //there maybe an optimization here where we grab the index lock outside of the loop
                //                 #inner_op
                //             }
                //         },
                //         OpAmInputToValue::ManyToOne(indices,val) => {
                //             for index in indices{
                //                 #inner_op
                //             }
                //         },
                //         OpAmInputToValue::ManyToMany(indices,vals) => {
                //             for (index,val) in indices.iter().zip(vals.iter()){
                //                 #inner_op
                //             }
                //         },
                //     }
                // }
                unsafe { results_u8.set_len(results_offset)};
                results_u8
            }
        }
        #[allow(non_snake_case)]
        fn #dist_am_buf_name(array: #lamellar::array::#byte_array_type) -> Arc<dyn #lamellar::array::BufferOp>{
                Arc::new(#buf_op_name{
                    data: array,
                    ops: Mutex::new(Vec::new()),
                    // new_ops: Mutex::new(Vec::new()),
                    cur_len: AtomicUsize::new(0),
                    complete: RwLock::new(Arc::new(AtomicBool::new(false))),
                    results_offset: RwLock::new(Arc::new(AtomicUsize::new(0))),
                    results: RwLock::new(Arc::new(Mutex::new(Vec::new()))),
                })
        }
        inventory::submit! {
            // #![crate = #lamellar]
            #lamellar::array::#reg_name{
                id: std::any::TypeId::of::<#typeident>(),
                op: #dist_am_buf_name,
            }
        }
    });
    expanded
}


fn gen_multi_val_multi_idx(op_type: proc_macro2::TokenStream, lock: &proc_macro2::TokenStream, op: proc_macro2::TokenStream) -> proc_macro2::TokenStream{
    quote! {
        #op_type =>{
            for elem in idx_vals{
                let index = elem.index;
                let val = elem.val;
                #lock
                #op
            }
        }
    }
}

fn gen_single_idx_multi_val(op_type: proc_macro2::TokenStream, lock: &proc_macro2::TokenStream, op: proc_macro2::TokenStream) -> proc_macro2::TokenStream{
    quote! {
        #op_type =>{
            for index in self.indices.iter(){
                let index = *index;
                #lock
                #op
            }
        }
    }
}

fn gen_multi_val_single_idx(op_type: proc_macro2::TokenStream, lock: &proc_macro2::TokenStream, op: proc_macro2::TokenStream) -> proc_macro2::TokenStream{
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

fn create_buf_ops2(
    typeident: syn::Type,
    array_type: syn::Ident,
    byte_array_type: syn::Ident,
    optypes: &Vec<OpType>,
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
            quote! { #val },                                                           //lhs
            quote! {slice[index].store(val, Ordering::SeqCst)},                        //assign
            quote! {
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
            },  //fetch_or
            quote! {
                res.push(slice[index].fetch_xor(val, Ordering::SeqCst));
            }, //fetch_xor
            quote! {slice[index].load(Ordering::SeqCst)},                              //load
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
            quote! {slice[index]},                                           //lhs
            quote! {slice[index] = val},                                     //assign
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
            quote! {slice[index]},                                           //load
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
            quote! {let mut slice = self.data.write_local_data();}, //this is the lock
        )
    } else if array_type == "GlobalLockArray" {
        (
            quote! {}, //no explicit lock since the slice handle is a lock guard
            quote! {let mut slice = self.data.async_write_local_data().await;}, //this is the lock
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
    let mut  all_match_stmts: Vec<(proc_macro2::TokenStream, fn(proc_macro2::TokenStream, & proc_macro2::TokenStream, proc_macro2::TokenStream) -> proc_macro2::TokenStream)> = vec![(multi_val_multi_idx_match_stmts,gen_multi_val_multi_idx),(single_val_multi_idx_match_stmts,gen_single_idx_multi_val),(multi_val_single_idx_match_stmts,gen_multi_val_single_idx)];
    for  (match_stmts, gen_fn) in all_match_stmts.iter_mut() {
        for optype in optypes {
            match optype {
                OpType::Arithmetic => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Add},&lock,quote!{ #lhs += val; })); 
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Sub},&lock,quote!{#lhs -= val; })); 
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Mul},&lock,quote!{#lhs *= val;})); 
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Div},&lock,quote!{#lhs /= val; })); 
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Rem},&lock,quote!{#lhs %= val; })); 
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Put},&lock,assign.clone())); 
                }
                ,
                OpType::Bitwise => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::And},&lock,quote!{#lhs &= val; })); 
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Or},&lock,quote!{#lhs |= val; })); 
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Xor},&lock,quote!{#lhs ^= val; })); 
                },
                OpType::Access => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Store},&lock,assign.clone())); 
                }
                OpType::Shift => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Shl},&lock,shl.clone()));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Shr},&lock,shr.clone()));
                },
                _ => {} //for fetch, readonly, and compex ops do nothing
            }
        }
        match_stmts.extend(quote!{
            _=> unreachable!("op: {:?} should not be possible in this context", self.op),
        });
    }

    let multi_val_multi_idx_match_stmts = all_match_stmts[0].0.clone();
    let single_val_multi_idx_match_stmts = all_match_stmts[1].0.clone();
    let multi_val_single_idx_match_stmts = all_match_stmts[2].0.clone();

    let  multi_val_multi_idx_fetch_match_stmts = quote! {};
    let  single_val_multi_idx_fetch_match_stmts = quote! {};
    let  multi_val_single_idx_fetch_match_stmts = quote! {};
    let mut  all_match_stmts: Vec<(proc_macro2::TokenStream, fn(proc_macro2::TokenStream, & proc_macro2::TokenStream, proc_macro2::TokenStream) -> proc_macro2::TokenStream)> = 
        vec![(multi_val_multi_idx_fetch_match_stmts,gen_multi_val_multi_idx),
             (single_val_multi_idx_fetch_match_stmts,gen_single_idx_multi_val),
             (multi_val_single_idx_fetch_match_stmts,gen_multi_val_single_idx)];
    for  (match_stmts, gen_fn) in all_match_stmts.iter_mut() {
        for optype in optypes {
            match optype {
                OpType::Arithmetic => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::FetchAdd},&lock,fetch_add.clone()));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::FetchSub},&lock,fetch_sub.clone()));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::FetchMul},&lock,fetch_mul.clone()));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::FetchDiv},&lock,fetch_div.clone()));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::FetchRem},&lock,fetch_rem.clone()));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Get},&lock,quote!{res.push(#load);}));
                },
                OpType::Bitwise => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::FetchAnd},&lock,fetch_and.clone()));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::FetchOr},&lock,fetch_or.clone()));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::FetchXor},&lock,fetch_xor.clone()));
                },
                OpType::Access => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Swap},&lock,swap.clone()));
                },
                OpType::ReadOnly => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::Load},&lock,quote!{res.push(#load);}));
                },
                OpType::Shift => {
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::FetchShl},&lock,fetch_shl.clone()));
                    match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::FetchShr},&lock,fetch_shr.clone()));
                   
                },
                _ => {} //dont handle result ops (CompEx,CompExEs) here
            }
        }
        match_stmts.extend(quote!{
            _=> unreachable!("op: {:?} should not be possible in this context", self.op),
        });
    }

    let multi_val_multi_idx_fetch_match_stmts = all_match_stmts[0].0.clone();
    let single_val_multi_idx_fetch_match_stmts = all_match_stmts[1].0.clone();
    let multi_val_single_idx_fetch_match_stmts = all_match_stmts[2].0.clone();

    let  multi_val_multi_idx_result_match_stmts = quote! {};
    let  single_val_multi_idx_result_match_stmts = quote! {};
    let  multi_val_single_idx_result_match_stmts = quote! {};
    let mut  all_match_stmts: Vec<(proc_macro2::TokenStream, fn(proc_macro2::TokenStream, & proc_macro2::TokenStream, proc_macro2::TokenStream) -> proc_macro2::TokenStream)> = 
        vec![(multi_val_multi_idx_result_match_stmts,gen_multi_val_multi_idx),
             (single_val_multi_idx_result_match_stmts,gen_single_idx_multi_val),
             (multi_val_single_idx_result_match_stmts,gen_multi_val_single_idx)];
    for  (match_stmts, gen_fn) in all_match_stmts.iter_mut() {
        for optype in optypes {
            match optype {
                OpType::CompEx =>  match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::CompareExchange(old)},&lock,compare_exchange.clone())),
                OpType::CompExEps =>  match_stmts.extend(gen_fn(quote! {ArrayOpCmd2::CompareExchangeEps(old,eps)},&lock,compare_exchange_eps.clone())),
                _ => {} //current only ops that return results are CompEx, CompExEps
            }
        }
        match_stmts.extend(quote!{
            _=> unreachable!("op: {:?} should not be possible in this context", self.op),
        });
    }

    let multi_val_multi_idx_result_match_stmts = all_match_stmts[0].0.clone();
    let single_val_multi_idx_result_match_stmts = all_match_stmts[1].0.clone();
    let multi_val_single_idx_result_match_stmts = all_match_stmts[2].0.clone();
    

    // let buf_op_name = quote::format_ident!("{}_{}_op_buf", array_type, type_to_string(&typeident));
    let multi_val_multi_idx_am_buf_name = quote::format_ident!("{}_{}_multi_val_multi_idx_am_buf", array_type, type_to_string(&typeident));
    let dist_multi_val_multi_idx_am_buf_name =
        quote::format_ident!("{}_{}_multi_val_multi_idx_am", array_type, type_to_string(&typeident));
    let multi_val_multi_idx_am_buf_fetch_name = quote::format_ident!("{}_{}_multi_val_multi_idx_am_buf_fetch", array_type, type_to_string(&typeident));
    let dist_multi_val_multi_idx_am_buf_fetch_name =
        quote::format_ident!("{}_{}_multi_val_multi_idx_am_fetch", array_type, type_to_string(&typeident));
    let multi_val_multi_idx_am_buf_result_name = quote::format_ident!("{}_{}_multi_val_multi_idx_am_buf_result", array_type, type_to_string(&typeident));
    let dist_multi_val_multi_idx_am_buf_result_name =
        quote::format_ident!("{}_{}_multi_val_multi_idx_am_result", array_type, type_to_string(&typeident));
    let multi_val_multi_idx_reg_name = quote::format_ident!("MultiValMultiIdxOps");

    let single_val_multi_idx_am_buf_name = quote::format_ident!("{}_{}_single_val_multi_idx_am_buf", array_type, type_to_string(&typeident));
    let dist_single_val_multi_idx_am_buf_name =
        quote::format_ident!("{}_{}_single_val_multi_idx_am", array_type, type_to_string(&typeident));
    let single_val_multi_idx_am_buf_fetch_name = quote::format_ident!("{}_{}_single_val_multi_idx_am_buf_fetch", array_type, type_to_string(&typeident));
    let dist_single_val_multi_idx_am_buf_fetch_name =
        quote::format_ident!("{}_{}_single_val_multi_idx_am_fetch", array_type, type_to_string(&typeident));
    let single_val_multi_idx_am_buf_result_name = quote::format_ident!("{}_{}_single_val_multi_idx_am_buf_result", array_type, type_to_string(&typeident));
    let dist_single_val_multi_idx_am_buf_result_name =
        quote::format_ident!("{}_{}_single_val_multi_idx_am_result", array_type, type_to_string(&typeident));
    let single_val_multi_idx_reg_name = quote::format_ident!("SingleValMultiIdxOps");

    let multi_val_single_idx_am_buf_name = quote::format_ident!("{}_{}_multi_val_single_idx_am_buf", array_type, type_to_string(&typeident));
    let dist_multi_val_single_idx_am_buf_name =
        quote::format_ident!("{}_{}_multi_val_single_idx_am", array_type, type_to_string(&typeident));
    let multi_val_single_idx_am_buf_fetch_name = quote::format_ident!("{}_{}_multi_val_single_idx_am_buf_fetch", array_type, type_to_string(&typeident));
    let dist_multi_val_single_idx_am_buf_fetch_name =
        quote::format_ident!("{}_{}_multi_val_single_idx_am_fetch", array_type, type_to_string(&typeident));
    let multi_val_single_idx_am_buf_result_name = quote::format_ident!("{}_{}_multi_val_single_idx_am_buf_result", array_type, type_to_string(&typeident));
    let dist_multi_val_single_idx_am_buf_result_name =
        quote::format_ident!("{}_{}_multi_val_single_idx_am_result", array_type, type_to_string(&typeident));
    let multi_val_single_idx_reg_name = quote::format_ident!("MultiValSingleIdxOps");


    

    if array_type != "ReadOnlyArray" {
        // Updating ops that dont return anything
        expanded.extend(quote! {
            #[#am_data(Debug)]
            struct #multi_val_multi_idx_am_buf_name{
                data: #lamellar::array::#array_type<#typeident>,
                op: #lamellar::array::ArrayOpCmd2<#typeident>,
                idx_vals: Vec<u8>,
            }
            #[#am]
            impl LamellarAM for #multi_val_multi_idx_am_buf_name{ //eventually we can return fetchs here too...
                async fn exec(&self) {
                    #slice
                    let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<#typeident>>())};
                    match self.op {
                        #multi_val_multi_idx_match_stmts
                    }
                }
            }
            #[allow(non_snake_case)]
            fn #dist_multi_val_multi_idx_am_buf_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd2<Vec<u8>>, idx_vals: Vec<u8>) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                    Arc::new(#multi_val_multi_idx_am_buf_name{
                        data: array.into(),
                        op: op.into(),
                        idx_vals: idx_vals,
                    })
            }
            inventory::submit! {
                #lamellar::array::#multi_val_multi_idx_reg_name{
                    id: (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),#lamellar::array::BatchReturnType::None),
                    op: #dist_multi_val_multi_idx_am_buf_name,
                }
            }            

            #[#am_data(Debug)]
            struct #single_val_multi_idx_am_buf_name{
                data: #lamellar::array::#array_type<#typeident>,
                op: #lamellar::array::ArrayOpCmd2<#typeident>,
                val: #typeident,
                indices: Vec<usize>,
                
            }
            #[#am]
            impl LamellarAM for #single_val_multi_idx_am_buf_name{ //eventually we can return fetchs here too...
                async fn exec(&self) {
                    #slice
                    let val = self.val;
                    match self.op {
                        #single_val_multi_idx_match_stmts
                    }
                }
            }
            #[allow(non_snake_case)]
            fn #dist_single_val_multi_idx_am_buf_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd2<Vec<u8>>, val: Vec<u8>, indicies: Vec<usize>) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                    let val_slice = unsafe {std::slice::from_raw_parts(val.as_ptr() as *const #typeident, std::mem::size_of::<#typeident>())};
                    let val = val_slice[0];
                    Arc::new(#single_val_multi_idx_am_buf_name{
                        data: array.into(),
                        op: op.into(),
                        val: val,
                        indices: indicies,
                    })
            }
            inventory::submit! {
                #lamellar::array::#single_val_multi_idx_reg_name{
                    id: (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),#lamellar::array::BatchReturnType::None),
                    op: #dist_single_val_multi_idx_am_buf_name,
                }
            }

            #[#am_data(Debug)]
            struct #multi_val_single_idx_am_buf_name{
                data: #lamellar::array::#array_type<#typeident>,
                op: #lamellar::array::ArrayOpCmd2<#typeident>,
                vals: Vec<u8>,
                index: usize,
                
            }
            #[#am]
            impl LamellarAM for #multi_val_single_idx_am_buf_name{ //eventually we can return fetchs here too...
                async fn exec(&self) {
                    #slice
                    let vals = unsafe {std::slice::from_raw_parts(self.vals.as_ptr() as *const #typeident, self.vals.len()/std::mem::size_of::<#typeident>())};
                    let index = self.index;
                    match self.op {
                        #multi_val_single_idx_match_stmts
                    }
                }
            }
            #[allow(non_snake_case)]
            fn #dist_multi_val_single_idx_am_buf_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd2<Vec<u8>>, vals: Vec<u8>, index: usize) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                    Arc::new(#multi_val_single_idx_am_buf_name{
                        data: array.into(),
                        op: op.into(),
                        vals: vals,
                        index: index,
                    })
            }
            inventory::submit! {
                #lamellar::array::#multi_val_single_idx_reg_name{
                    id: (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),#lamellar::array::BatchReturnType::None),
                    op: #dist_multi_val_single_idx_am_buf_name,
                }
            }
        });

        // ops that return a result
        if optypes.contains(&OpType::CompEx) || optypes.contains(&OpType::CompExEps){
            expanded.extend(quote! {
                #[#am_data(Debug)]
                struct #multi_val_multi_idx_am_buf_result_name{
                    data: #lamellar::array::#array_type<#typeident>,
                    op: #lamellar::array::ArrayOpCmd2<#typeident>,
                    idx_vals: Vec<u8>,
                }
                #[#am]
                impl LamellarAM for #multi_val_multi_idx_am_buf_result_name{ //eventually we can return fetchs here too...
                    async fn exec(&self) -> Vec<Result<#typeident,#typeident>> {
                        #slice
                        let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<#typeident>>())};
                        let mut res = Vec::new();
                        match self.op {
                            #multi_val_multi_idx_result_match_stmts
                        }
                        res
                    }
                }
                #[allow(non_snake_case)]
                fn #dist_multi_val_multi_idx_am_buf_result_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd2<Vec<u8>>, idx_vals: Vec<u8>) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                        Arc::new(#multi_val_multi_idx_am_buf_result_name{
                            data: array.into(),
                            op: op.into(),
                            idx_vals: idx_vals,
                        })
                }
                inventory::submit! {
                    #lamellar::array::#multi_val_multi_idx_reg_name{
                        id: (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),#lamellar::array::BatchReturnType::Result),
                        op: #dist_multi_val_multi_idx_am_buf_result_name,
                    }
                }

                #[#am_data(Debug)]
                struct #single_val_multi_idx_am_buf_result_name{
                    data: #lamellar::array::#array_type<#typeident>,
                    op: #lamellar::array::ArrayOpCmd2<#typeident>,
                    val: #typeident,
                    indices: Vec<usize>,
                    
                }
                #[#am]
                impl LamellarAM for #single_val_multi_idx_am_buf_result_name{ //eventually we can return fetchs here too...
                    async fn exec(&self) -> Vec<Result<#typeident,#typeident>> {
                        #slice
                        let val = self.val;
                        let mut res = Vec::new();
                        match self.op {
                            #single_val_multi_idx_result_match_stmts
                        }
                        res
                    }
                }
                #[allow(non_snake_case)]
                fn #dist_single_val_multi_idx_am_buf_result_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd2<Vec<u8>>, val: Vec<u8>, indicies: Vec<usize>) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                        let val_slice = unsafe {std::slice::from_raw_parts(val.as_ptr() as *const #typeident, std::mem::size_of::<#typeident>())};
                        let val = val_slice[0];
                        Arc::new(#single_val_multi_idx_am_buf_result_name{
                            data: array.into(),
                            op: op.into(),
                            val: val,
                            indices: indicies,
                        })
                }
                inventory::submit! {
                    #lamellar::array::#single_val_multi_idx_reg_name{
                        id: (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),#lamellar::array::BatchReturnType::Result),
                        op: #dist_single_val_multi_idx_am_buf_result_name,
                    }
                }

                #[#am_data(Debug)]
                struct #multi_val_single_idx_am_buf_result_name{
                    data: #lamellar::array::#array_type<#typeident>,
                    op: #lamellar::array::ArrayOpCmd2<#typeident>,
                    vals: Vec<u8>,
                    index: usize,
                    
                }
                #[#am]
                impl LamellarAM for #multi_val_single_idx_am_buf_result_name{ //eventually we can return fetchs here too...
                    async fn exec(&self) -> Vec<Result<#typeident,#typeident>>  {
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
                fn #dist_multi_val_single_idx_am_buf_result_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd2<Vec<u8>>, vals: Vec<u8>, index: usize) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                        Arc::new(#multi_val_single_idx_am_buf_result_name{
                            data: array.into(),
                            op: op.into(),
                            vals: vals,
                            index: index,
                        })
                }
                inventory::submit! {
                    #lamellar::array::#multi_val_single_idx_reg_name{
                        id: (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),#lamellar::array::BatchReturnType::Result),
                        op: #dist_multi_val_single_idx_am_buf_result_name,
                    }
                }
            });
        }
    }
    //ops that return a value

    expanded.extend(quote! {
        #[#am_data(Debug)]
        struct #multi_val_multi_idx_am_buf_fetch_name{
            data: #lamellar::array::#array_type<#typeident>,
            op: #lamellar::array::ArrayOpCmd2<#typeident>,
            idx_vals: Vec<u8>,
        }
        #[#am]
        impl LamellarAM for #multi_val_multi_idx_am_buf_fetch_name{ //eventually we can return fetchs here too...
            async fn exec(&self) -> Vec<#typeident> {
                #slice
                let idx_vals = unsafe {std::slice::from_raw_parts(self.idx_vals.as_ptr() as *const IdxVal<#typeident>, self.idx_vals.len()/std::mem::size_of::<IdxVal<#typeident>>())};
                let mut res = Vec::new();
                match self.op {
                    #multi_val_multi_idx_fetch_match_stmts
                }
                res
            }
        }
        #[allow(non_snake_case)]
        fn #dist_multi_val_multi_idx_am_buf_fetch_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd2<Vec<u8>>, idx_vals: Vec<u8>) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                Arc::new(#multi_val_multi_idx_am_buf_fetch_name{
                    data: array.into(),
                    op: op.into(),
                    idx_vals: idx_vals,
                })
        }
        inventory::submit! {
            #lamellar::array::#multi_val_multi_idx_reg_name{
                id: (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),#lamellar::array::BatchReturnType::Vals),
                op: #dist_multi_val_multi_idx_am_buf_fetch_name,
            }
        }

        #[#am_data(Debug)]
        struct #single_val_multi_idx_am_buf_fetch_name{
            data: #lamellar::array::#array_type<#typeident>,
            op: #lamellar::array::ArrayOpCmd2<#typeident>,
            val: #typeident,
            indices: Vec<usize>,
            
        }
        #[#am]
        impl LamellarAM for #single_val_multi_idx_am_buf_fetch_name{ //eventually we can return fetchs here too...
            async fn exec(&self) -> Vec<#typeident>{
                #slice
                let val = self.val;
                let mut res = Vec::new();
                match self.op {
                    #single_val_multi_idx_fetch_match_stmts
                }
                res
            }
        }
        #[allow(non_snake_case)]
        fn #dist_single_val_multi_idx_am_buf_fetch_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd2<Vec<u8>>, val: Vec<u8>, indicies: Vec<usize>) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                let val_slice = unsafe {std::slice::from_raw_parts(val.as_ptr() as *const #typeident, std::mem::size_of::<#typeident>())};
                let val = val_slice[0];
                Arc::new(#single_val_multi_idx_am_buf_fetch_name{
                    data: array.into(),
                    op: op.into(),
                    val: val,
                    indices: indicies,
                })
        }
        inventory::submit! {
            #lamellar::array::#single_val_multi_idx_reg_name{
                id: (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),#lamellar::array::BatchReturnType::Vals),
                op: #dist_single_val_multi_idx_am_buf_fetch_name,
            }
        }

        #[#am_data(Debug)]
        struct #multi_val_single_idx_am_buf_fetch_name{
            data: #lamellar::array::#array_type<#typeident>,
            op: #lamellar::array::ArrayOpCmd2<#typeident>,
            vals: Vec<u8>,
            index: usize,
            
        }
        #[#am]
        impl LamellarAM for #multi_val_single_idx_am_buf_fetch_name{ //eventually we can return fetchs here too...
            async fn exec(&self) -> Vec<#typeident> {
                #slice
                let vals = unsafe {std::slice::from_raw_parts(self.vals.as_ptr() as *const #typeident, self.vals.len()/std::mem::size_of::<#typeident>())};
                let index = self.index;
                let mut res = Vec::new();
                match self.op {
                    #multi_val_single_idx_fetch_match_stmts
                }
                res
            }
        }
        #[allow(non_snake_case)]
        fn #dist_multi_val_single_idx_am_buf_fetch_name(array: #lamellar::array::LamellarByteArray, op: #lamellar::array::ArrayOpCmd2<Vec<u8>>, vals: Vec<u8>, index: usize) -> Arc<dyn RemoteActiveMessage + Sync + Send>{
                Arc::new(#multi_val_single_idx_am_buf_fetch_name{
                    data: array.into(),
                    op: op.into(),
                    vals: vals,
                    index: index,
                })
        }
        inventory::submit! {
            #lamellar::array::#multi_val_single_idx_reg_name{
                id: (std::any::TypeId::of::<#byte_array_type>(),std::any::TypeId::of::<#typeident>(),#lamellar::array::BatchReturnType::Vals),
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
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

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
        quote::format_ident!("ReadOnlyByteArrayWeak"),
        &ro_optypes,
        rt,
    );
    expanded.extend(buf_op_impl);
    let buf_op_impl = create_buf_ops2(
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
        quote::format_ident!("UnsafeByteArrayWeak"),
        &optypes,
        rt,
    );
    expanded.extend(buf_op_impl);

    let buf_op_impl = create_buf_ops2(
        typeident.clone(),
        quote::format_ident!("UnsafeArray"),
        quote::format_ident!("UnsafeByteArray"),
        &optypes,
        rt,
    );
    expanded.extend(buf_op_impl);

    for (array_type, byte_array_type_weak, byte_array_type) in atomic_array_types {
        let buf_op_impl = create_buf_ops(
            typeident.clone(),
            array_type.clone(),
            byte_array_type_weak.clone(),
            &optypes,
            rt,
        );
        expanded.extend(buf_op_impl);
        let buf_op_impl = create_buf_ops2(
            typeident.clone(),
            array_type.clone(),
            byte_array_type.clone(),
            &optypes,
            rt,
        );
        expanded.extend(buf_op_impl);
    }

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            use __lamellar::array::prelude::*;
            use __lamellar::active_messaging::prelude::*;
            use __lamellar::memregion::prelude::*;
            use __lamellar::darc::prelude::*;
            use __lamellar::array::{
                ArrayOpCmd,
                ArrayOpCmd2,
                OpResultOffsets,
                RemoteOpAmInputToValue,
                PeOpResults,
                OpResults,
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
            #expanded
        };
    };
    if lamellar == "crate" {
        expanded
    } else {
        user_expanded
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
            impl crate::array::operations::ArrayOps for #typeident {}
            impl ElementArithmeticOps for #typeident {}
            impl ElementComparePartialEqOps for #typeident {}
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
            })
        }
        output.extend(create_buffered_ops(
            the_type.clone(),
            op_types.clone(),
            native,
            true,
        ));
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
        OpType::Bitwise
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
    let mut output = quote! {};

    let input = parse_macro_input!(input as syn::DeriveInput);
    let name = input.ident;
    let the_type: syn::Type = syn::parse_quote!(#name);

    let mut op_types = vec![OpType::ReadOnly, OpType::Access];
    let mut element_wise_trait_impls = quote!{};

    for attr in input.attrs {
        if attr.to_token_stream().to_string().contains("array_ops") {
            if let Ok(temp) = attr.parse_meta() {
                match temp {
                    syn::Meta::Path(p) => println!("Not expected {p:?}"),
                    syn::Meta::NameValue(nv) => println!("Not expected {nv:?}"),
                    syn::Meta::List(l) => {
                        for item in l.nested {
                            match item.to_token_stream().to_string().as_str() {
                                "Arithmetic" => {
                                    op_types.push(OpType::Arithmetic);
                                    element_wise_trait_impls.extend(
                                        quote! {
                                            impl lamellar::ElementArithmeticOps for #the_type {}
                                        }
                                    )
                                },
                                "CompExEps" => {
                                    op_types.push(OpType::CompExEps);
                                    element_wise_trait_impls.extend(
                                        quote! {
                                            impl lamellar::ElementComparePartialEqOps for #the_type {}
                                        }
                                    )
                                },
                                "CompEx" => {
                                    op_types.push(OpType::CompEx);
                                    element_wise_trait_impls.extend(
                                        quote! {
                                            impl lamellar::ElementCompareEqOps for #the_type {}
                                        }
                                    )
                                },
                                "Bitwise" => {
                                    op_types.push(OpType::Bitwise);
                                    element_wise_trait_impls.extend(
                                        quote! {
                                            impl lamellar::ElementBitWiseOps for #the_type {}
                                        }
                                    )
                                },
                                "Shift" => {
                                    op_types.push(OpType::Shift);
                                    element_wise_trait_impls.extend(
                                        quote! {
                                            impl lamellar::ElementShiftOps for #the_type {}
                                        }
                                    )
                                },
                                "All" => {
                                    op_types.push(OpType::Arithmetic);
                                    op_types.push(OpType::CompEx);
                                    op_types.push(OpType::Bitwise);
                                    op_types.push(OpType::Shift);
                                    element_wise_trait_impls.extend(
                                        quote! {
                                            impl lamellar::ElementArithmeticOps for #the_type {}
                                            impl lamellar::ElementComparePartialEqOps for #the_type {}
                                            impl lamellar::ElementCompareEqOps for #the_type {}
                                            impl lamellar::ElementBitWiseOps for #the_type {}
                                            impl lamellar::ElementShiftOps for #the_type {}
                                        }
                                    )
                                }
                                &_ => abort!(item, "unexpected array op type, valid types are: Arithmetic, CompEx, CompExEps, Bitwise, Shift, All"),
                            }
                        }
                    }
                }
            }
        }
    }

    output.extend(quote! {
        impl lamellar::_ArrayOps for #the_type {}
        #element_wise_trait_impls
    });
    output.extend(create_buffered_ops(
        the_type.clone(),
        op_types,
        false,
        false,
    ));
    TokenStream::from(output)
}


