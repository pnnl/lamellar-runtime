use crate::parse::ReductionArgs;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{quote, quote_spanned};
use syn::parse_macro_input;
use syn::spanned::Spanned;

fn create_reduction(
    typeident: syn::Ident,
    reduction: String,
    op: proc_macro2::TokenStream,
    array_types: &Vec<syn::Ident>,
) -> proc_macro2::TokenStream {
    let lamellar = quote::format_ident!("__lamellar");

    let am_data: syn::Path = syn::parse("lamellar::AmData".parse().unwrap()).unwrap();
    let am: syn::Path = syn::parse("lamellar::am".parse().unwrap()).unwrap();
    let reduction = quote::format_ident!("{:}", reduction);
    let reduction_gen = quote::format_ident!("{:}_{:}_reduction_gen", typeident, reduction);
    let reduction_id_gen = quote::format_ident!("{:}_{:}_reduction_id", typeident, reduction);

    let reduction_name = quote::format_ident!("{:}_{:}_reduction", typeident, reduction);

    // Recursive branch: left/right return Option<Vec<u8>>; deserialize, apply op, re-serialize.
    let array_impls = quote! {
        #[allow(non_camel_case_types)]
        #[#am_data(Clone,Debug,AmGroup(false))]
        struct #reduction_name {
            data: #lamellar::array::LamellarByteArray,
            start_pe: usize,
            end_pe: usize,
        }

        #[#am(AmGroup(false))]
        impl LamellarAM for #reduction_name {
            async fn exec(&self) -> Vec<u8> {
                if self.start_pe == self.end_pe {
                    let local = self.data.local_data::<#typeident>().await;
                    match local.reduce(#op){
                        None => Vec::new(),
                        Some(v) => #lamellar::serialize(&v, true).expect("failed to serialize reduction result"),
                    }

                } else {
                    let mid_pe = (self.start_pe + self.end_pe) / 2;
                    let op = #op;
                    let left = __lamellar_team.spawn_am_pe(self.start_pe, #reduction_name {
                        data: self.data.clone(), start_pe: self.start_pe, end_pe: mid_pe
                    });
                    let right = __lamellar_team.spawn_am_pe(mid_pe + 1, #reduction_name {
                        data: self.data.clone(), start_pe: mid_pe + 1, end_pe: self.end_pe
                    });
                    let left_bytes = left.await;
                    let right_bytes = right.await;
                    if left_bytes.is_empty() && right_bytes.is_empty() {
                        Vec::new()
                    } else if left_bytes.is_empty() {
                        right_bytes
                    } else if right_bytes.is_empty() {
                        left_bytes
                    } else {
                        let left_val = #lamellar::deserialize::<#typeident>(&left_bytes, true).expect("merge_scalar des left");
                        let right_val = #lamellar::deserialize::<#typeident>(&right_bytes, true).expect("merge_scalar des right");
                        let res = op(left_val, right_val);
                        #lamellar::serialize(&res, true).expect("failed to serialize reduction result")
                    }
                }
            }
        }
    };

    let mut gen_match_stmts = quote! {};

    gen_match_stmts.extend(quote! {
        #lamellar::array::LamellarByteArray::NativeAtomicArray(_) => panic!("this type is not a native atomic"),
        #lamellar::array::LamellarByteArray::NetworkAtomicArray(_) => panic!("this type is not a network atomic"),
    });
    for array_type in array_types {
        gen_match_stmts.extend(quote! {
            #lamellar::array::LamellarByteArray::#array_type(_) => std::sync::Arc::new(#reduction_name {
                data: data.clone(), start_pe: 0, end_pe: num_pes - 1
            }),
        });
    }

    let expanded = quote! {
        fn  #reduction_gen (data: #lamellar::array::LamellarByteArray, num_pes: usize)
        -> std::sync::Arc<dyn #lamellar::active_messaging::RemoteActiveMessage + Sync + Send>{
            match data{
                #gen_match_stmts
            }

        }

        fn  #reduction_id_gen () -> std::any::TypeId{
            std::any::TypeId::of::<#typeident>()
        }


        #lamellar::inventory::submit! {
            // #![crate = #lamellar]
            #lamellar::array::ReduceKey{
                id: #reduction_id_gen,
                name: stringify!(#reduction),//.to_string(),
                gen: #reduction_gen
            }
        }

        #array_impls
    };

    let user_expanded = quote_spanned! {expanded.span()=>
        const _: () = {
            extern crate lamellar as __lamellar;
            use __lamellar::active_messaging::prelude::*;
            // use __lamellar::array::{LamellarArrayPut};
            #expanded
        };
    };
    if lamellar == "crate" {
        expanded
    } else {
        user_expanded
    }
}

pub(crate) fn __register_reduction(item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(item as ReductionArgs);
    let mut output = quote! {};
    let array_types: Vec<syn::Ident> = vec![
        quote::format_ident!("LocalLockArray"),
        quote::format_ident!("GlobalLockArray"),
        quote::format_ident!("AtomicArray"),
        quote::format_ident!("GenericAtomicArray"),
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
        // println!("{:?}", closure);

        output.extend(create_reduction(
            ty.path.segments[0].ident.clone(),
            args.name.to_string(),
            quote! {#closure},
            &array_types,
        ));
    }
    TokenStream::from(output)
}

// pub(crate) fn __generate_reductions_for_type(item: TokenStream) -> TokenStream {
//     let mut output = quote! {};
//     let read_array_types: Vec<syn::Ident> = vec![
//         quote::format_ident!("LocalLockArray"),
//         quote::format_ident!("AtomicArray"),
//         quote::format_ident!("GenericAtomicArray"),
//         quote::format_ident!("UnsafeArray"),
//         quote::format_ident!("ReadOnlyArray"),
//     ];

//     for t in item.to_string().split(",").collect::<Vec<&str>>() {
//         let typeident = quote::format_ident!("{:}", t.trim());
//         output.extend(create_reduction(
//             typeident.clone(),
//             "sum".to_string(),
//             quote! {
//                 let data_slice = <lamellar::LamellarMemoryRegion<#typeident> as lamellar::RegisteredMemoryRegion>::as_slice(&self.data).unwrap();
//                 let first = data_slice.first().unwrap().clone();
//                 data_slice[1..].iter().fold(first,|acc,val|{ acc+val } )
//             },
//             &read_array_types,
//             false,
//             false
//         ));
//         output.extend(create_reduction(
//             typeident.clone(),
//             "prod".to_string(),
//             quote! {
//                 let data_slice = <lamellar::LamellarMemoryRegion<#typeident> as lamellar::RegisteredMemoryRegion>::as_slice(&self.data).unwrap();
//                 let first = data_slice.first().unwrap().clone();
//                 data_slice[1..].iter().fold(first,|acc,val|{ acc*val } )
//             },
//             &read_array_types,
//             false,
//             false
//         ));
//         output.extend(create_reduction(
//             typeident.clone(),
//             "max".to_string(),
//             quote! {
//                 *<lamellar::LamellarMemoryRegion<#typeident> as lamellar::RegisteredMemoryRegion>::as_slice(&self.data).unwrap().iter().max().unwrap()
//             },
//             &read_array_types,
//             false,
//             false
//         ));
//     }

//     TokenStream::from(output)
// }

// fn pod_type_variant(type_str: &str) -> proc_macro2::TokenStream {
//     match type_str.trim() {
//         "u8"    => quote! { crate::array::PodType::U8    },
//         "u16"   => quote! { crate::array::PodType::U16   },
//         "u32"   => quote! { crate::array::PodType::U32   },
//         "u64"   => quote! { crate::array::PodType::U64   },
//         "usize" => quote! { crate::array::PodType::Usize },
//         "u128"  => quote! { crate::array::PodType::U128  },
//         "i8"    => quote! { crate::array::PodType::I8    },
//         "i16"   => quote! { crate::array::PodType::I16   },
//         "i32"   => quote! { crate::array::PodType::I32   },
//         "i64"   => quote! { crate::array::PodType::I64   },
//         "isize" => quote! { crate::array::PodType::Isize },
//         "i128"  => quote! { crate::array::PodType::I128  },
//         "f32"   => quote! { crate::array::PodType::F32   },
//         "f64"   => quote! { crate::array::PodType::F64   },
//         other   => panic!("unknown primitive type for builtin reduction: {}", other),
//     }
// }

// pub(crate) fn __generate_reductions_for_type_rt(item: TokenStream) -> TokenStream {
//     let mut output = quote! {};
//     let items = item
//         .to_string()
//         .split(",")
//         .map(|i| i.to_owned())
//         .collect::<Vec<String>>();
//     let _native = if let Ok(val) = syn::parse_str::<syn::LitBool>(&items[0]) {
//         val.value
//     } else {
//         panic!("first argument of generate_ops_for_type expects 'true' or 'false' specifying whether types are native atomics");
//     };

//     // Emit only ReduceKey inventory entries — the single PodBuiltinReductionAm struct
//     // in reduce_helpers.rs handles all types and operations via runtime dispatch.
//     for t in items[1..].iter() {
//         let t = t.trim().to_string();
//         let typeident = quote::format_ident!("{}", t.trim());
//         let pod_variant = pod_type_variant(&t);

//         for (op_name, builtin_op) in &[
//             ("sum",  quote! { crate::array::BuiltinOp::Sum  }),
//             ("prod", quote! { crate::array::BuiltinOp::Prod }),
//             ("max",  quote! { crate::array::BuiltinOp::Max  }),
//             ("min",  quote! { crate::array::BuiltinOp::Min  }),
//         ] {
//             let reduction_gen = quote::format_ident!("{}_{}_{}_reduction_gen", typeident, op_name, "pod");
//             let reduction_id_gen = quote::format_ident!("{}_{}_{}_reduction_id", typeident, op_name, "pod");
//             let op_name_lit = *op_name;
//             let pod = pod_variant.clone();
//             let bop = builtin_op.clone();

//             output.extend(quote! {
//                 fn #reduction_gen(data: crate::array::LamellarByteArray, num_pes: usize)
//                     -> std::sync::Arc<dyn crate::active_messaging::RemoteActiveMessage + Sync + Send>
//                 {
//                     std::sync::Arc::new(crate::array::PodBuiltinReductionAm {
//                         data,
//                         start_pe: 0,
//                         end_pe: num_pes - 1,
//                         pod_type: #pod,
//                         op: #bop,
//                     })
//                 }

//                 fn #reduction_id_gen() -> std::any::TypeId {
//                     std::any::TypeId::of::<#typeident>()
//                 }

//                 crate::inventory::submit! {
//                     crate::array::ReduceKey {
//                         id: #reduction_id_gen,
//                         name: #op_name_lit,
//                         gen: #reduction_gen,
//                     }
//                 }
//             });
//         }
//     }
//     TokenStream::from(output)
// }
