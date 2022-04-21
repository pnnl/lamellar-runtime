

use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::parse_macro_input;
use syn::spanned::Spanned;



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

fn native_atomic_slice(
    typeident: &syn::Ident,
    lamellar: &proc_macro2::Ident,
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    match typeident.to_string().as_str() {
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
        _ => panic!("this should never happen"),
    }
}

fn create_buf_ops(
    typeident: syn::Ident,
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
    let mut expanded = quote! {};
    let (lhs, assign, load, lock, slice) = if array_type == "GenericAtomicArray" {
        (
            quote! {slice[index]},
            quote! {slice[index] = val},
            quote! {slice[index]},
            quote! {let _lock = self.data.lock_index(index);},
            quote! {let mut slice = unsafe{self.data.__local_as_mut_slice()};},
        )
    } else if array_type == "NativeAtomicArray" {
        let (slice, val) = native_atomic_slice(&typeident, &lamellar);
        (
            quote! { #val },
            quote! {slice[index].store(val, Ordering::SeqCst)},
            quote! {slice[index].load(Ordering::SeqCst)},
            quote! {},
            // quote! {use crate::array::native_atomic::AsNativeAtomic;let mut slice = unsafe{self.data.__local_as_mut_slice()}; }
            quote! { #slice },
        )
    } else if array_type == "LocalLockAtomicArray" {
        (   
            quote! {slice[index]},
            quote! {slice[index] = val},
            quote! {slice[index]},
            quote! {},
            quote! {let mut slice = self.data.write_local_data();},
        )
    } else {
        (
            quote! {slice[index]},
            quote! {slice[index] = val},
            quote! {slice[index]},
            quote! {},
            quote! {let mut slice = unsafe{self.data.mut_local_data()};},
        )
    };

    let mut match_stmts = quote! {};
    for optype in optypes {
        match optype {
            OpType::Arithmetic => match_stmts.extend(quote! {
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
                ArrayOpCmd::Mul=>{ #lhs *= val},
                ArrayOpCmd::FetchMul=>{
                    results_slice[fetch_index] = orig;
                    fetch_index+=1;
                    #lhs *= val
                },
                ArrayOpCmd::Div=>{ #lhs /= val },
                ArrayOpCmd::FetchDiv=>{
                    results_slice[fetch_index] = orig;
                    fetch_index+=1;
                    #lhs /= val
                },
                ArrayOpCmd::Put => {#assign},
                ArrayOpCmd::Get =>
                    {results_slice[fetch_index]=orig;
                    fetch_index+=1
                }
            }),
            OpType::Bitwise => match_stmts.extend(quote! {
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
            }),
            OpType::Atomic => match_stmts.extend(quote! {
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
            }),
        }
    }

    let buf_op_name = quote::format_ident!("{}_{}_op_buf", array_type, typeident);
    let am_buf_name = quote::format_ident!("{}_{}_am_buf", array_type, typeident);
    let dist_am_buf_name = quote::format_ident!("{}_{}_am_buf", array_type, typeident);
    let reg_name = quote::format_ident!("{}OpBuf", array_type);

    let inner_op=quote!{
        let index = *index;
        let val = *val;
        #lock //this will get dropped at end of loop
        let orig = #load;
        // println!("before op: {:?} index: {:?} val {:?} fetch_index {:?} orig {:?}",op,index,val,fetch_index,orig);

        match op{
        # match_stmts
        _ => {panic!("shouldnt happen {:?}",op)}
        }
    };

    expanded.extend(quote! {
        struct #buf_op_name{
            data: #lamellar::array::#array_type<#typeident>,
            // ops: Mutex<Vec<(ArrayOpCmd,Vec<(usize,#typeident)>)>>,
            ops: Mutex<Vec<(ArrayOpCmd,#lamellar::array::OpAmInputToValue<#typeident>)>>,
            cur_len: AtomicUsize, //this could probably just be a normal usize cause we only update it after we get ops lock
            complete: RwLock<Arc<AtomicBool>>,
            result_cnt:  RwLock<Arc<AtomicUsize>>,
            results:  RwLock<PeOpResults>,
        }
        #[#am_data]
        struct #am_buf_name{
            // wait: Darc<AtomicUsize>,
            data: #lamellar::array::#array_type<#typeident>,
            // ops: Vec<(ArrayOpCmd,Vec<(usize,#typeident)>)>,
            ops: Vec<(ArrayOpCmd,#lamellar::array::OpAmInputToValue<#typeident>)>,
            num_fetch_ops: usize,
            orig_pe: usize,
        }
        impl #lamellar::array::BufferOp for #buf_op_name{
            fn add_ops(&self, op: ArrayOpCmd, op_data_ptr: *const u8) -> (bool,Arc<AtomicBool>){
                let op_data = unsafe{(&*(op_data_ptr as *const #lamellar::array::InputToValue<'_,#typeident>)).as_op_am_input()};
                let mut buf = self.ops.lock();
                let first = buf.len() == 0;
                let _temp = self.cur_len.fetch_add(op_data.len(),Ordering::SeqCst);
                buf.push((op,op_data));
                (first,self.complete.read().clone())
            }
            fn add_fetch_ops(&self, pe: usize, op: ArrayOpCmd, op_data_ptr: *const u8, req_ids: &Vec<usize>, res_map: OpResults) -> (bool,Arc<AtomicBool>,Option<OpResultIndices>){
                let op_data = unsafe{(&*(op_data_ptr as *const #lamellar::array::InputToValue<'_,#typeident>)).as_op_am_input()};
                let mut res_indicies = vec![];
                let mut buf = self.ops.lock();
                for rid in req_ids{
                    res_indicies.push((*rid,self.result_cnt.read().fetch_add(1,Ordering::SeqCst)));
                }
                let first = buf.len() == 0;
                let _temp = self.cur_len.fetch_add(op_data.len(),Ordering::SeqCst);
                buf.push((op,op_data));
                res_map.insert(pe,self.results.read().clone());
                (first,self.complete.read().clone(),Some(res_indicies))
            }
            fn into_arc_am(&self,sub_array: std::ops::Range<usize>)-> (Vec<Arc<dyn RemoteActiveMessage + Send + Sync>>,usize,Arc<AtomicBool>, PeOpResults){
                // println!("into arc am {:?}",stringify!(#am_buf_name));
                let mut ams: Vec<Arc<dyn RemoteActiveMessage + Send + Sync>> = Vec::new();
                let mut buf = self.ops.lock();
                // println!("buf len {:?}",buf.len());
                let mut ops = Vec::new();
                let len = self.cur_len.load(Ordering::SeqCst);
                self.cur_len.store(0,Ordering::SeqCst);
                std::mem::swap(&mut ops,  &mut buf);
                let mut complete = Arc::new(AtomicBool::new(false));
                std::mem::swap(&mut complete, &mut self.complete.write());
                // println!(" complete{:?}",complete);
                // let mut results = Arc::new(RwLock::new(HashMap::new()));
                let mut results = Arc::new(Mutex::new(Vec::new()));

                std::mem::swap(&mut results, &mut self.results.write());
                // println!(" here");
                let mut num_fetch_ops = self.result_cnt.read().swap(0,Ordering::SeqCst);

                let mut cur_size = 0;
                let mut op_i = ops.len() as isize-1;
                // let mut inner_i = ops[op_i as usize].1[0].len() as isize-1;
                // Vec<(ArrayOpCmd,)> == op_i;
                // Vec<(Vec<usize>,#typeident)>
                
                while op_i >= 0 {
                    while op_i >= 0 && (cur_size + ops[op_i as usize].1.num_bytes() < 10000000) {
                        cur_size += ops[op_i as usize].1.num_bytes() ;    
                        op_i -= 1isize;
                    }
                    
                    let new_ops = ops.split_off((op_i+ 1 as isize) as usize);
                    // println!("cur_size: {:?} i {:?} len {:?} ops_len {:?}",cur_size ,op_i, new_ops.len(),ops.len());
                    let mut am = #am_buf_name{
                        // wait: wait.clone(),
                        data: self.data.sub_array(sub_array.clone()),
                        ops: new_ops,
                        num_fetch_ops: num_fetch_ops,
                        orig_pe: self.data.my_pe(),
                    };
                    ams.push(Arc::new(am));
                    cur_size = 0;
                }
                ams.reverse();
                // println!("ams len: {:?}",ams.len());
                // }
                (ams,len,complete,results)
            }
        }
        #[#am]
        impl LamellarAM for #am_buf_name{ //eventually we can return fetchs here too...
            fn exec(&self) -> Vec<u8>{
                // self.data.process_ops(&self.ops);
                // let mut prev = self.wait.fetch_add(1,Ordering::SeqCst);
                // let mut val = self.wait.load(Ordering::SeqCst);
                // println!("from: {:?} {:?} {:?} {:?}",self.orig_pe,prev,val,stringify!(#array_type));
                // while val < lamellar::num_pes && prev <= val{
                //     std::thread::yield_now();
                //     prev = val;
                //     val = self.wait.load(Ordering::SeqCst)
                // }
                // println!("from: {:?} num ops {:?}  {:?}",self.orig_pe,self.ops.len() ,stringify!(#array_type));
                #slice
                // println!("slice len {:?}",slice.len());
                // println!("opslen {:?} num fetch ops {:?}",self.ops.len(),self.num_fetch_ops);
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
                for (op, ops) in &self.ops { //(ArrayOpCmd,OpAmInputToValue)
                    match ops{
                        OpAmInputToValue::OneToOne(index,val) => {
                            #inner_op
                        },
                        OpAmInputToValue::OneToMany(index,vals) => {
                            for val in vals{ //there maybe an optimization here where we grab the index lock outside of the loop
                                #inner_op
                            }
                        },
                        OpAmInputToValue::ManyToOne(indices,val) => {
                            for index in indices{
                                #inner_op
                            }
                        },
                        OpAmInputToValue::ManyToMany(indices,vals) => {
                            for (index,val) in indices.iter().zip(vals.iter()){
                                #inner_op
                            }
                        },
                    }
                // for (op, ops) in &self.ops{ //(ArrayOpCmd,Vec<(Vec<usize>,#typeident)>)
                //     // println!("op: {:?} len {:?} {:?}",op,ops.len(),ops);
                //     for (idxs,val) in ops{ // Vec<(Vec<usize>,#typeident)>
                //         for index in idxs{ //Vec<usize>
                //             // println!("before op: {:?} index: {:?} val {:?} fetch_index {:?}",op,index,val,fetch_index);
                //             let index = *index;
                //             let val = *val;
                //             #lock //this will get dropped at end of loop
                //             let orig = #load;
                //             // println!("before op: {:?} index: {:?} val {:?} fetch_index {:?} orig {:?}",op,index,val,fetch_index,orig);

                //             match op{
                //             # match_stmts
                //             _ => {panic!("shouldnt happen {:?}",op)}
                //             }
                //             // println!("done op: {:?} index: {:?} val {:?} fetch_index {:?}",op,index,val,fetch_index);
                //         }
                //     }
                }
                // println!("buff ops exec: {:?} {:?} {:?}",results_u8.len(),u8_len,self.num_fetch_ops);
                // println!("results {:?}",results_slice);
                // println!("results_u8 {:?}",results_u8);
                unsafe { results_u8.set_len(fetch_index * std::mem::size_of::<#typeident>())};
                // let val = self.wait.fetch_sub(1,Ordering::SeqCst);
                // println!("trying to leave {:?} {:?}",self.orig_pe, val);
                // while self.wait.load(Ordering::SeqCst) > 0{
                //     std::thread::yield_now();
                // }
                results_u8
            }
        }
        #[allow(non_snake_case)]
        fn #dist_am_buf_name(array: #lamellar::array::#byte_array_type) -> Arc<dyn #lamellar::array::BufferOp>{
            // println!("{:}",stringify!(#dist_fn_name));
            Arc::new(#buf_op_name{
                data: unsafe {array.into()},
                ops: Mutex::new(Vec::new()),
                cur_len: AtomicUsize::new(0),
                complete: RwLock::new(Arc::new(AtomicBool::new(false))),
                result_cnt: RwLock::new(Arc::new(AtomicUsize::new(0))),
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
    Atomic,
}

fn create_buffered_ops(
    typeident: syn::Ident,
    bitwise: bool,
    native: bool,
    rt: bool,
) -> proc_macro2::TokenStream {
    let lamellar = if rt {
        quote::format_ident!("crate")
    } else {
        quote::format_ident!("__lamellar")
    };

    let mut atomic_array_types: Vec<(syn::Ident, syn::Ident)> = vec![
        (
            quote::format_ident!("LocalLockAtomicArray"),
            quote::format_ident!("LocalLockAtomicByteArray"),
        ),
        (
            quote::format_ident!("GenericAtomicArray"),
            quote::format_ident!("GenericAtomicByteArray"),
        ),
    ];

    if native {
        atomic_array_types.push((
            quote::format_ident!("NativeAtomicArray"),
            quote::format_ident!("NativeAtomicByteArray"),
        ));
    }

    let mut expanded = quote! {};

    let mut optypes = vec![OpType::Arithmetic];
    if bitwise {
        optypes.push(OpType::Bitwise);
    }
    let buf_op_impl = create_buf_ops(
        typeident.clone(),
        quote::format_ident!("UnsafeArray"),
        quote::format_ident!("UnsafeByteArray"),
        &optypes,
        rt,
        bitwise,
    );
    expanded.extend(buf_op_impl);
    optypes.push(OpType::Atomic);
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
            use __lamellar::array::{AtomicArray,AtomicByteArray,GenericAtomicArray,NativeAtomicArray,LocalLockAtomicArray,LocalLockAtomicByteArray,LocalArithmeticOps,LocalAtomicOps,ArrayOpCmd,LamellarArrayPut,OpResultIndices,PeOpResults,OpResults};
            // #bitwise_mod
            use __lamellar::array;
            // #bitwise_mod
            use __lamellar::Darc;
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

#[cfg(feature = "non-buffered-array-ops")]
fn create_ops(
    typeident: syn::Ident,
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
            quote::format_ident!("LocalLockAtomicByteArray"),
        ),
        (
            quote::format_ident!("AtomicArray"),
            quote::format_ident!("AtomicByteArray"),
        ),
        (
            quote::format_ident!("GenericAtomicArray"),
            quote::format_ident!("GenericAtomicByteArray"),
        ),
        (
            quote::format_ident!("UnsafeArray"),
            quote::format_ident!("UnsafeByteArray"),
        ),
    ];

    if native {
        write_array_types.push((
            quote::format_ident!("NativeAtomicArray"),
            quote::format_ident!("NativeAtomicByteArray"),
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
            quote::format_ident!("LocalLockAtomicByteArray"),
        ),
        (
            quote::format_ident!("AtomicArray"),
            quote::format_ident!("AtomicByteArray"),
        ),
        (
            quote::format_ident!("GenericAtomicArray"),
            quote::format_ident!("GenericAtomicByteArray"),
        ),
    ];

    if native {
        atomic_array_types.push((
            quote::format_ident!("NativeAtomicArray"),
            quote::format_ident!("NativeAtomicByteArray"),
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
        let typeident = quote::format_ident!("{:}", t.trim());
        output.extend(quote! {impl Dist for #typeident {}});
        #[cfg(feature = "non-buffered-array-ops")]
        output.extend(create_ops(typeident.clone(), bitwise, native, false));
        #[cfg(not(feature = "non-buffered-array-ops"))]
        output.extend(create_buffered_ops(
            typeident.clone(),
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
        let typeident = quote::format_ident!("{:}", t.trim());
        output.extend(quote! {impl Dist for #typeident {}});
        #[cfg(feature = "non-buffered-array-ops")]
        output.extend(create_ops(typeident.clone(), bitwise, native, true));
        #[cfg(not(feature = "non-buffered-array-ops"))]
        output.extend(create_buffered_ops(
            typeident.clone(),
            bitwise,
            native,
            true,
        ));
        // output.extend(gen_atomic_rdma(typeident.clone(), true));
    }
    TokenStream::from(output)
}

pub(crate) fn __derive_arrayops(input: TokenStream) -> TokenStream {
    let mut output = quote! {};

    let input = parse_macro_input!(input as syn::DeriveInput);
    let name = input.ident;

    #[cfg(feature = "non-buffered-array-ops")]
    output.extend(create_ops(name.clone(), false, false, false));
    #[cfg(not(feature = "non-buffered-array-ops"))]
    output.extend(create_buffered_ops(name.clone(), false, false, false));
    TokenStream::from(output)
}

