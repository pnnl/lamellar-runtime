use proc_macro::TokenStream;
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

#[cfg(feature = "non-buffered-array-ops")]
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
                quote! { Box<dyn #lamellar::LamellarRequest<Output = #typeident>  > }
            }
            false => {
                quote! {Option<Box<dyn #lamellar::LamellarRequest<Output = ()>  >>}
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
    _bitwise: bool,
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
        fetch_and,
        fetch_or,
        load,
        swap,
        compare_exchange,
        compare_exchange_eps,
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
            quote! {#res_t res_t[0] = slice[index].fetch_and(val, Ordering::SeqCst);}, //fetch_and
            quote! {#res_t res_t[0] = slice[index].fetch_or(val, Ordering::SeqCst);},  //fetch_or
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
                // println!("native atomic");
                // if val == 4 || val == 55527435{
                //     println!(" am i insane? {} {} {} {:?} {:?}",val,index,slice[index].load(Ordering::SeqCst),std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
                // }
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
        )
    } else if array_type == "ReadOnlyArray" {
        (
            quote! { panic!("assign a valid op for Read Only Arrays");},                                           //lhs
            quote! { panic!("assign/store not a valid op for Read Only Arrays");},                                     //assign
            quote! { panic!("fetch_add not a valid op for Read Only Arrays"); }, //fetch_add -- we lock the index before this point so its actually atomic
            quote! { panic!("fetch_sub not a valid op for Read Only Arrays"); }, //fetch_sub --we lock the index before this point so its actually atomic
            quote! { panic!("fetch_mul not a valid op for Read Only Arrays"); }, //fetch_mul --we lock the index before this point so its actually atomic
            quote! { panic!("fetch_div not a valid op for Read Only Arrays"); }, //fetch_div --we lock the index before this point so its actually atomic
            quote! { panic!("fetch_and not a valid op for Read Only Arrays"); }, //fetch_and --we lock the index before this point so its actually atomic
            quote! { panic!("fetch_or not a valid op for Read Only Arrays"); }, //fetch_or --we lock the index before this point so its actually atomic
            quote! {slice[index]},                                           //load
            quote! { panic!("swap not a valid op for Read Only Arrays"); }, //swap we lock the index before this point so its actually atomic
            quote! { panic!("compare exchange not a valid op for Read Only Arrays"); }, // compare_exchange -- we lock the index before this point so its actually atomic
            quote! { panic!("compare exchange eps not a valid op for Read Only Arrays"); }, //compare exchange epsilon
        )
    }
    else{
        (
            quote! {slice[index]},                                           //lhs
            quote! {slice[index] = val},                                     //assign
            quote! {#res_t res_t[0] =  slice[index]; slice[index] += val; }, //fetch_add -- we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] -= val; }, //fetch_sub --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] *= val; }, //fetch_mul --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] /= val; }, //fetch_div --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] &= val; }, //fetch_and --we lock the index before this point so its actually atomic
            quote! {#res_t res_t[0] =  slice[index]; slice[index] |= val; }, //fetch_or --we lock the index before this point so its actually atomic
            quote! {slice[index]},                                           //load
            quote! {#res_t res_t[0] =  slice[index]; slice[index] = val; }, //swap we lock the index before this point so its actually atomic
            quote! {  // compare_exchange -- we lock the index before this point so its actually atomic
                // println!("old : {:?} val : {:?} s[i] {:?}", *old, val, slice[index]);
                // println!("not native atomic, {:?}",stringify!(#array_type));
                // let selected_val = val == 4 as #typeident || val == 55527435 as #typeident || val == 22 as #typeident || val == 290162221 as #typeident || val == 10000016 as #typeident ;
                // if selected_val{
                //     println!(" am i insane? val: {} index: {} old: {} cur: {} {} {:?} {:?}",val,index, old, slice[index], old == slice[index],std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
                // }
                 let old = if old == slice[index]{
                    // println!("the same!");
                    // if selected_val{
                    //     println!(" the same!!! val: {} index: {} old: {} cur: {} {} {:?} {:?}",val,index, old, slice[index], old == slice[index],std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
                    // }
                    slice[index] = val;
                    results_u8[results_offset] = 0;
                    results_offset+=1;
                    old
                } else {
                    // println!("different!");
                    // if selected_val{
                    //     println!(" different!!! val: {} index: {} old: {} cur: {} {} {:?} {:?}",val,index, old, slice[index], old == slice[index],std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
                    // }
                    results_u8[results_offset] = 1;
                    results_offset+=1;
                    slice[index]
                };
                // if selected_val{
                //     println!(" after am i insane? val: {} index: {} old: {} cur: {} {:?} {:?}",val,index, old, slice[index],std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
                // }
                #res_t res_t[0] = old;
            },
            quote! { //compare exchange epsilon
                // let same = if *old > slice[index] {
                //     *old - slice[index] < *eps
                // }
                // else{
                //     slice[index] - *old < *eps
                // };
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
    } else if array_type == "LocalLockAtomicArray" {
        (
            quote! {}, //no explicit lock since the slice handle is a lock guard
            quote! {let mut slice = self.data.write_local_data();}, //this is the lock
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
                ArrayOpCmd::Put => {#assign},
                ArrayOpCmd::Get =>{
                    #res_t res_t[0] = #load;

                }
            }),
            OpType::Bitwise => match_stmts.extend(quote! {
                ArrayOpCmd::And=>{#lhs &= val},
                ArrayOpCmd::FetchAnd=>{
                    // println!("{:?} {:?}", val,#load);
                    #fetch_and
                    // println!("{:?} {:?}\n", val,#load);
                },
                ArrayOpCmd::Or=>{#lhs |= val},
                ArrayOpCmd::FetchOr=>{

                    #fetch_or

                },
            }),
            OpType::Access => match_stmts.extend(quote! {
                ArrayOpCmd::Store=>{
                    #assign
                },
                ArrayOpCmd::Swap=>{
                    #swap
                },
                ArrayOpCmd::CompareExchange(old) =>{
                    #compare_exchange
                }
                ArrayOpCmd::CompareExchangeEps(old,eps) =>{
                    #compare_exchange_eps
                }
            }),
            OpType::ReadOnly => match_stmts.extend(quote! {
                ArrayOpCmd::Load=>{
                    #res_t res_t[0] =  #load;
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
        // println!("before op: {:?} index: {:?} val {:?} results_index {:?} orig {:?}",op,index,val,results_u8,orig);

        match op{
        # match_stmts
        _ => {panic!("shouldnt happen {:?}",op)}
        }
    };

    expanded.extend(quote! {
        struct #buf_op_name{
            data: #lamellar::array::#byte_array_type,
            // ops: Mutex<Vec<(ArrayOpCmd<#typeident>,#lamellar::array::OpAmInputToValue<#typeident>)>>,
            new_ops: Mutex<Vec<(LocalMemoryRegion<u8>,usize)>>,
            cur_len: AtomicUsize, //this could probably just be a normal usize cause we only update it after we get ops lock
            complete: RwLock<Arc<AtomicBool>>,
            results_offset: RwLock<Arc<AtomicUsize>>,
            results:  RwLock<PeOpResults>,
        }
        #[#am_data(Debug)]
        struct #am_buf_name{
            data: #lamellar::array::#array_type<#typeident>,
            // ops: Vec<(ArrayOpCmd<#typeident>,#lamellar::array::OpAmInputToValue<#typeident>)>,
            // new_ops: Vec<LocalMemoryRegion<u8>>,
            ops2: LocalMemoryRegion<u8>,
            res_buf_size: usize,
            orig_pe: usize,
        }
        impl #lamellar::array::BufferOp for #buf_op_name{
            #[tracing::instrument(skip_all)]
            fn add_ops(&self, op_ptr: *const u8, op_data_ptr: *const u8, team: Pin<Arc<LamellarTeamRT>>) -> (bool,Arc<AtomicBool>){
                let span1 = tracing::trace_span!("convert");
                let _guard = span1.enter();
                let op_data = unsafe{(&*(op_data_ptr as *const #lamellar::array::InputToValue<'_,#typeident>)).as_op_am_input()};
                let op = unsafe{*(op_ptr as *const ArrayOpCmd<#typeident>)};
                drop(_guard);
                let span2 = tracing::trace_span!("lock");
                let _guard = span2.enter();
                let mut bufs = self.new_ops.lock();

                // let mut buf = self.ops.lock();

                drop(_guard);
                let span3 = tracing::trace_span!("update");
                let _guard = span3.enter();
                let first = bufs.len() == 0;
                let op_size = op.num_bytes();
                let data_size = op_data.num_bytes();
                if first {
                    bufs.push((team.alloc_local_mem_region(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE,op_size+data_size)),0));
                }
                else {
                    if bufs.last().unwrap().1 + op_size+data_size > #lamellar::array::OPS_BUFFER_SIZE{
                        bufs.push((team.alloc_local_mem_region(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size)),0));
                    }
                }
                let mut buf: &mut (LocalMemoryRegion<u8>, usize) = bufs.last_mut().unwrap();
                
                let _temp = self.cur_len.fetch_add(op_data.len(),Ordering::SeqCst);
                let mut buf_slice = unsafe{buf.0.as_mut_slice().unwrap()};
                
                buf.1 += op.to_bytes(&mut buf_slice[(buf.1)..]);
                buf.1 += op_data.to_bytes(&mut buf_slice[(buf.1)..]);
                // if buf.1 > #lamellar::array::OPS_BUFFER_SIZE {
                //     println!("{} {} {}",buf.1,op_size,data_size);
                // }
                // buf.push((op,op_data));
                drop(_guard);
                (first,self.complete.read().clone())
            }
            #[tracing::instrument(skip_all)]
            fn add_fetch_ops(&self, pe: usize, op_ptr: *const u8, op_data_ptr: *const u8, req_ids: &Vec<usize>, res_map: OpResults, team: Pin<Arc<LamellarTeamRT>>) -> (bool,Arc<AtomicBool>,Option<OpResultOffsets>){
                // println!("add_fetch_op {} {:?} {:?}",pe,std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
                let op_data = unsafe{(&*(op_data_ptr as *const #lamellar::array::InputToValue<'_,#typeident>)).as_op_am_input()};
                let op = unsafe{*(op_ptr as *const ArrayOpCmd<#typeident>)};
                let mut res_offsets = vec![];
                // let mut buf = self.ops.lock();
                let mut bufs = self.new_ops.lock();
                // println!("add_fetch_op got lock {} {:?} {:?}",pe,std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));

                for rid in req_ids{
                    let res_size = op.result_size();
                    res_offsets.push((*rid,self.results_offset.read().fetch_add(res_size,Ordering::SeqCst),res_size));
                }
                // println!("{:?}",res_offsets);
                // let first = buf.len() == 0;
                // buf.push((op,op_data));
                let first = bufs.len() == 0;
                let op_size = op.num_bytes();
                let data_size = op_data.num_bytes();
                // println!("add_fetch_ops {:?} {:?}",op_size,data_size);
                if first {
                    bufs.push((team.alloc_local_mem_region(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size)),0));
                }
                else {
                    if bufs.last().unwrap().1 + op_size+data_size > #lamellar::array::OPS_BUFFER_SIZE{
                        // println!("here");
                        bufs.push((team.alloc_local_mem_region(std::cmp::max(#lamellar::array::OPS_BUFFER_SIZE, op_size+data_size)),0));
                    }
                }
                let mut buf: &mut (LocalMemoryRegion<u8>, usize) = bufs.last_mut().unwrap();
                
                let _temp = self.cur_len.fetch_add(op_data.len(),Ordering::SeqCst);
                let mut buf_slice = unsafe{buf.0.as_mut_slice().unwrap()};
                
                buf.1 += op.to_bytes(&mut buf_slice[(buf.1)..]);
                buf.1 += op_data.to_bytes(&mut buf_slice[(buf.1)..]);
                res_map.insert(pe,self.results.read().clone());
                // println!("add_fetch_op leaving {} {:?} {:?} {:?} {:?}",pe,std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH),res_offsets[0],res_offsets.last());
                (first,self.complete.read().clone(),Some(res_offsets))
            }
            #[tracing::instrument(skip_all)]
            fn into_arc_am(&self, pe: usize, sub_array: std::ops::Range<usize>)-> (Vec<Arc<dyn RemoteActiveMessage + Sync + Send>>,usize,Arc<AtomicBool>, PeOpResults){
                // println!("into arc am {:?}",stringify!(#am_buf_name));
                // println!("into_arc_am {} {:?} {:?}",pe,std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));

                let mut ams: Vec<Arc<dyn RemoteActiveMessage + Sync + Send>> = Vec::new();
                let mut bufs = self.new_ops.lock();
                // println!("into_arc_am got lock {} {:?} {:?}",pe,std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));

                // println!("buf len {:?}",buf.len());
                let mut ops = Vec::new();
                let len = self.cur_len.load(Ordering::SeqCst);
                // println!("into_arc_am: cur_len {}",len);
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
                for (lmr,size) in ops.drain(..){

                    let mut am = #am_buf_name{
                        // wait: wait.clone(),
                        data: data.sub_array(sub_array.clone()),
                        ops2: lmr.sub_region(0..size),
                        res_buf_size: result_buf_size,
                        orig_pe: data.my_pe(),
                    };
                    ams.push(Arc::new(am));
                }
                // println!("into_arc_am leaving {} num_ams {} {:?} {:?}",pe,ams.len(),std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));

                (ams,len,complete,results)
            }
        }

        impl #am_buf_name{
            async fn get_ops(&self, team: &std::sync::Arc<#lamellar::LamellarTeam>) -> #lamellar::LocalMemoryRegion<u8>{
                // println!("get_ops {:?}",self.ops2.len());
                unsafe{
                    let serialized_ops = if self.ops2.data_local(){
                        self.ops2.clone()
                    }
                    else{
                        let serialized_ops = team.alloc_local_mem_region::<u8>(self.ops2.len());
                        let local_slice = serialized_ops.as_mut_slice().unwrap();
                        local_slice[self.ops2.len()- 1] = 255u8;
                        // self.ops2.get_unchecked(0, serialized_ops.clone());
                        self.ops2.iget(0, serialized_ops.clone());

                        while local_slice[self.ops2.len()- 1] == 255u8 {
                            // async_std::task::yield_now().await;
                            std::thread::yield_now();
                        }
                        serialized_ops
                    };
                    serialized_ops
                    // self.ops2.iget(0,serialized_ops.clone());
                    // #lamellar::deserialize(serialized_ops.as_mut_slice().unwrap(),false).unwrap()
                }
            }

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
                // println!("trying to get lock {:?} {:?}",std::thread::current().id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH));
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
                // println!("{:?} {:?} {:?}",results_u8.len(),u8_len,results_offset);
                // let mut results_slice = unsafe{std::slice::from_raw_parts_mut(results_u8.as_mut_ptr() as *mut #typeident,self.num_fetch_ops)};
                
                let local_ops = self.get_ops(&lamellar::team).await;
                let local_ops_slice = local_ops.as_slice().unwrap();
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
                            // println!("starting index {:?} end index {:?} =--{:?} {:?}",vals[0], vals[vals.len()-1],vals.len(),indices.len());
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
                // println!("{:?} {:?} {:?} {:?}",results_u8.len(),u8_len,results_offset,results_u8);
                results_u8
            }
        }
        #[allow(non_snake_case)]
        fn #dist_am_buf_name(array: #lamellar::array::#byte_array_type) -> Arc<dyn #lamellar::array::BufferOp>{
                Arc::new(#buf_op_name{
                    data: array,
                    // ops: Mutex::new(Vec::new()),
                    new_ops: Mutex::new(Vec::new()),
                    cur_len: AtomicUsize::new(0),
                    complete: RwLock::new(Arc::new(AtomicBool::new(false))),
                    results_offset: RwLock::new(Arc::new(AtomicUsize::new(0))),
                    results: RwLock::new(Arc::new(Mutex::new(Vec::new()))),
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
    Access,
    ReadOnly
}

fn create_buffered_ops(
    typeident: syn::Type,
    bitwise: bool,
    native: bool,
    rt: bool,
) -> proc_macro2::TokenStream {
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let mut atomic_array_types: Vec<(syn::Ident, syn::Ident)> = vec![(
        quote::format_ident!("LocalLockAtomicArray"),
        quote::format_ident!("LocalLockAtomicByteArrayWeak"),
    )];

    if native {
        atomic_array_types.push((
            quote::format_ident!("NativeAtomicArray"),
            quote::format_ident!("NativeAtomicByteArrayWeak"),
        ));
    } else {
        atomic_array_types.push((
            quote::format_ident!("GenericAtomicArray"),
            quote::format_ident!("GenericAtomicByteArrayWeak"),
        ));
    }

    let mut expanded = quote! {};

    let mut optypes = vec![OpType::ReadOnly];//, vec![OpType::Arithmetic, OpType::Access];

    let buf_op_impl = create_buf_ops(
        typeident.clone(),
        quote::format_ident!("ReadOnlyArray"),
        quote::format_ident!("ReadOnlyByteArrayWeak"),
        &optypes,
        rt,
        bitwise,
    );
    expanded.extend(buf_op_impl);
    optypes.push(OpType::Arithmetic);
    optypes.push(OpType::Access);
    if bitwise {
        optypes.push(OpType::Bitwise);
    }
    let buf_op_impl = create_buf_ops(
        typeident.clone(),
        quote::format_ident!("UnsafeArray"),
        quote::format_ident!("UnsafeByteArrayWeak"),
        &optypes,
        rt,
        bitwise,
    );
    expanded.extend(buf_op_impl);

    for (array_type, byte_array_type) in atomic_array_types {
        let buf_op_impl = create_buf_ops(
            typeident.clone(),
            array_type.clone(),
            byte_array_type.clone(),
            &optypes,
            rt,
            bitwise,
        );
        expanded.extend(buf_op_impl)
    }

    

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            use __lamellar::array::{
                AtomicArray,AtomicByteArray,AtomicByteArrayWeak,
                GenericAtomicArray,
                NativeAtomicArray,
                LocalLockAtomicArray,LocalLockAtomicByteArray,LocalLockAtomicByteArrayWeak,
                LocalArithmeticOps,LocalAtomicOps,
                UnsafeArray, UnsafeByteArray, UnsafeByteArrayWeak,
                ArrayOpCmd,
                LamellarArrayPut,
                OpResultOffsets,
                RemoteOpAmInputToValue,
                PeOpResults,OpResults,
                OpAmInputToValue};
            // #bitwise_mod
            use __lamellar::array;
            use __lamellar::LamellarTeamRT;
            // #bitwise_mod
            use __lamellar::Darc;
            use __lamellar::LamellarArray;
            use __lamellar::LamellarRequest;
            use __lamellar::RemoteActiveMessage;
            // use __lamellar::memregion::{RemoteMemoryRegion};
            use __lamellar::LocalMemoryRegion;
            use __lamellar::RemoteMemoryRegion;
            use std::sync::Arc;
            use parking_lot::{Mutex,RwLock};
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

#[cfg(feature = "non-buffered-array-ops")]
fn create_ops(
    typeident: syn::Type,
    bitwise: bool,
    native: bool,
    rt: bool,
) -> proc_macro2::TokenStream {
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let mut write_array_types: Vec<(syn::Ident, syn::Ident)> = vec![
        (
            quote::format_ident!("LocalLockAtomicArray"),
            quote::format_ident!("LocalLockAtomicByteArrayWeak"),
        ),
        (
            quote::format_ident!("AtomicArray"),
            quote::format_ident!("AtomicByteArrayWeak"),
        ),
        (
            quote::format_ident!("GenericAtomicArray"),
            quote::format_ident!("GenericAtomicByteArrayWeak"),
        ),
        (
            quote::format_ident!("UnsafeArray"),
            quote::format_ident!("UnsafeByteArrayWeak"),
        ),
    ];

    if native {
        write_array_types.push((
            quote::format_ident!("NativeAtomicArray"),
            quote::format_ident!("NativeAtomicByteArrayWeak"),
        ));
    }
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
            quote::format_ident!("LocalLockAtomicArray"),
            quote::format_ident!("LocalLockAtomicByteArrayWeak"),
        ),
        (
            quote::format_ident!("AtomicArray"),
            quote::format_ident!("AtomicByteArrayWeak"),
        ),
        (
            quote::format_ident!("GenericAtomicArray"),
            quote::format_ident!("GenericAtomicByteArrayWeak"),
        ),
    ];

    if native {
        atomic_array_types.push((
            quote::format_ident!("NativeAtomicArray"),
            quote::format_ident!("NativeAtomicByteArrayWeak"),
        ));
    }
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
            use __lamellar::array::{AtomicArray,AtomicByteArray,GenericAtomicArray,NativeAtomicArray,LocalLockAtomicArray,LocalLockAtomicByteArray,LocalArithmeticOps,LocalAtomicOps,ArrayOpCmd,LamellarArrayPut};
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

pub(crate) fn __generate_ops_for_type(item: TokenStream) -> TokenStream {
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
    let native = false; // since this is a user defined type, we assume it does not have native support for atomic operations
    for t in items[1..].iter() {
        let the_type = syn::parse_str::<syn::Type>(&t).unwrap();
        // let (wrapped_impl, wrapped_type) = create_wrapped_type(&the_type, bitwise);
        // output.extend(wrapped_impl);
        println!("{:?}", the_type);
        // let typeident = quote::format_ident!("{:}", t.trim());
        output.extend(quote! {impl Dist for #the_type {}});
        #[cfg(feature = "non-buffered-array-ops")]
        output.extend(create_ops(the_type.clone(), bitwise, native, false));
        #[cfg(not(feature = "non-buffered-array-ops"))]
        output.extend(create_buffered_ops(
            the_type.clone(),
            bitwise,
            native,
            false,
        ));
        // output.extend(gen_atomic_rdma(typeident.clone(), false));
    }
    TokenStream::from(output)
}

pub(crate) fn __generate_ops_for_type_rt(item: TokenStream) -> TokenStream {
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
    let native = if let Ok(val) = syn::parse_str::<syn::LitBool>(&items[1]) {
        val.value
    } else {
        panic! ("first argument of generate_ops_for_type expects 'true' or 'false' specifying whether types are native atomics");
    };
    for t in items[2..].iter() {
        let the_type = syn::parse_str::<syn::Type>(&t).unwrap();
        let typeident = quote::format_ident!("{:}", t.trim());
        output.extend(quote! {impl Dist for #typeident {}});
        #[cfg(feature = "non-buffered-array-ops")]
        output.extend(create_ops(typeident.clone(), bitwise, native, true));
        #[cfg(not(feature = "non-buffered-array-ops"))]
        output.extend(create_buffered_ops(the_type.clone(), bitwise, native, true));
        // output.extend(gen_atomic_rdma(typeident.clone(), true));
    }
    TokenStream::from(output)
}

// fn create_wrapped_type(base_type:& syn::Type, bitwise: bool) -> (proc_macro2::TokenStream  ,syn::Type){
//     let wrapped_type = quote::format_ident!("wrapped_{}", type_to_string(base_type));
//     (if bitwise{
//         quote!{
//             const _: () = {
//                 extern crate lamellar as __lamellar;
//                 __lamellar::custom_derive! {
//                     #[derive(Send, Sync, Copy, Debug, NewtypeAddAssign, NewtypeSubAssign, NewtypeMulAssign, NewtypeDivAssign, NewtypeBitAndAssign, NewtypeBitOrAssign )]
//                     pub struct #wrapped_type(#base_type);
//                 }
//             };
//         }
//     }
//     else{
//         quote!{
//             const _: () = {
//                 extern crate lamellar as __lamellar;
//                 use __lamellar::custom_derive::*;
//                 use __lamellar::newtype_derive::*;
//                 custom_derive! {
//                     #[derive( Copy, Debug, NewtypeAddAssign, NewtypeSubAssign, NewtypeMulAssign, NewtypeDivAssign)]
//                     pub struct #wrapped_type(#base_type);
//                 }
//             };
//         }
//     }, syn::parse_str::<syn::Type>(wrapped_type.to_string().as_str()).unwrap())
// }

pub(crate) fn __derive_arrayops(input: TokenStream) -> TokenStream {
    let mut output = quote! {};

    let input = parse_macro_input!(input as syn::DeriveInput);
    let name = input.ident;
    let the_type: syn::Type = syn::parse_quote!(#name);
    // let (wrapped_impl, wrapped_type) = create_wrapped_type(&the_type, false);
    // output.extend(wrapped_impl);

    #[cfg(feature = "non-buffered-array-ops")]
    output.extend(create_ops(the_type.clone(), false, false, false));
    #[cfg(not(feature = "non-buffered-array-ops"))]
    output.extend(create_buffered_ops(the_type.clone(), false, false, false));
    TokenStream::from(output)
}