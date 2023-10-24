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
    rt: bool,
    native: bool,
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
    let reduction_id_gen = quote::format_ident!("{:}_{:}_reduction_id", typeident, reduction);

    let mut gen_match_stmts = quote! {};
    let mut array_impls = quote! {};

    if !native {
        gen_match_stmts.extend(quote!{
            #lamellar::array::LamellarByteArray::NativeAtomicArray(_) => panic!("this type is not a native atomic"),
        });
    }
    for array_type in array_types {
        let reduction_name =
            quote::format_ident!("{:}_{:}_{:}_reduction", array_type, typeident, reduction);

        gen_match_stmts.extend(quote!{
            #lamellar::array::LamellarByteArray::#array_type(inner) => std::sync::Arc::new(#reduction_name{
                data: unsafe {inner.clone().into()} , start_pe: 0, end_pe: num_pes-1}),
        });

        let iter_chain = if array_type == "AtomicArray"
            || array_type == "GenericAtomicArray"
            || array_type == "NativeAtomicArray"
        {
            quote! {.map(|elem| elem.load())}
        } else {
            quote! {.copied()}
        };

        array_impls.extend(quote! {
            #[allow(non_camel_case_types)]
            #[#am_data(Clone,Debug)]
            struct #reduction_name{
                data: #lamellar::array::#array_type<#typeident>,
                start_pe: usize,
                end_pe: usize,
            }

            #[#am]
            impl LamellarAM for #reduction_name{
                async fn exec(&self) -> #typeident{
                    // println!("{}",stringify!(#array_type));
                    if self.start_pe == self.end_pe{
                        // println!("[{:?}] root {:?} {:?}",__lamellar_current_pe,self.start_pe, self.end_pe);
                        let timer = std::time::Instant::now();
                        #[allow(unused_unsafe)]
                        let data_slice = unsafe {self.data.local_data()};
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
                        let left = __lamellar_team.exec_am_pe( self.start_pe,  #reduction_name { data: self.data.clone(), start_pe: self.start_pe, end_pe: mid_pe});//.into_future();
                        let right = __lamellar_team.exec_am_pe( mid_pe+1, #reduction_name { data: self.data.clone(), start_pe: mid_pe+1, end_pe: self.end_pe});//.into_future();
                        let res = op(left.await,right.await);

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
            false,
            false, // "lamellar".to_string(),
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

pub(crate) fn __generate_reductions_for_type_rt(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    let items = item
        .to_string()
        .split(",")
        .map(|i| i.to_owned())
        .collect::<Vec<String>>();
    let native = if let Ok(val) = syn::parse_str::<syn::LitBool>(&items[0]) {
        val.value
    } else {
        panic! ("first argument of generate_ops_for_type expects 'true' or 'false' specifying whether types are native atomics");
    };

    let mut read_array_types: Vec<syn::Ident> = vec![
        quote::format_ident!("LocalLockArray"),
        quote::format_ident!("GlobalLockArray"),
        quote::format_ident!("AtomicArray"),
        quote::format_ident!("GenericAtomicArray"),
        quote::format_ident!("UnsafeArray"),
        quote::format_ident!("ReadOnlyArray"),
    ];
    if native {
        read_array_types.push(quote::format_ident!("NativeAtomicArray"));
    }

    for t in items[1..].iter() {
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
            native,
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "prod".to_string(),
            quote! {
                |acc, val| { acc * val }
            },
            &read_array_types,
            true,
            native,
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "max".to_string(),
            quote! {
                |val1, val2| { if val1 > val2 {val1} else {val2} }
            },
            &read_array_types,
            true,
            native,
        ));
        output.extend(create_reduction(
            typeident.clone(),
            "min".to_string(),
            quote! {
                |val1, val2| { if val1 < val2 {val1} else {val2} }
            },
            &read_array_types,
            true,
            native,
        ));
    }
    TokenStream::from(output)
}
