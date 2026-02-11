use crate::field_info::FieldInfo;
use crate::gen_am::create_am_struct;
use crate::gen_am_group::create_am_group_structs;

use std::collections::HashMap;

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::parse_macro_input;
use syn::punctuated::Punctuated;

fn check_attrs(attrs: &Vec<syn::Attribute>) -> (bool, bool, Vec<syn::Attribute>) {
    let mut static_field = false;
    let attrs = attrs
        .iter()
        .filter_map(|a| {
            if a.to_token_stream()
                .to_string()
                .contains("#[AmGroup(static)]")
            {
                static_field = true;
                None
            } else {
                Some(a.clone())
            }
        })
        .collect::<Vec<_>>();

    let mut darc_iter = false;
    let attrs = attrs
        .iter()
        .filter_map(|a| {
            if a.to_token_stream().to_string().contains("#[Darc(iter)]") {
                darc_iter = true;
                None
            } else {
                Some(a.clone())
            }
        })
        .collect::<Vec<_>>();
    (static_field, darc_iter, attrs)
}

fn process_fields(
    args: Punctuated<syn::Meta, syn::Token![,]>,
    the_fields: &mut syn::Fields,
    lamellar: &proc_macro2::TokenStream,
    local: bool,
) -> (
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    FieldInfo,
    FieldInfo,
    bool,
) {
    let mut fields = FieldInfo::new();
    let mut static_fields = FieldInfo::new();

    for field in the_fields {
        if let syn::Type::Path(ref ty) = field.ty {
            if let Some(_seg) = ty.path.segments.first() {
                if local {
                    fields.add_field(field.clone(), false);
                } else {
                    let (static_field, darc_iter, attrs) = check_attrs(&field.attrs);
                    field.attrs = attrs;
                    if static_field {
                        static_fields.add_field(field.clone(), darc_iter);
                    } else {
                        fields.add_field(field.clone(), darc_iter);
                    }
                }
            }
        } else if let syn::Type::Tuple(ref _ty) = field.ty {
            let (static_field, darc_iter, attrs) = check_attrs(&field.attrs);
            field.attrs = attrs;
            if static_field {
                static_fields.add_field(field.clone(), darc_iter);
            } else {
                fields.add_field(field.clone(), darc_iter);
            }
        } else if let syn::Type::Reference(ref _ty) = field.ty {
            if !local {
                panic!("references are not supported in Remote Active Messages");
            } else {
                fields.add_field(field.clone(), false);
            }
        } else {
            if !local {
                panic!("unsupported type in Remote Active Message {:?}", field.ty);
            }
            fields.add_field(field.clone(), false);
        }
    }
    let mut lamellar = lamellar.clone();
    if lamellar.to_string() == "__lamellar" {
        lamellar = quote! {lamellar};
    }
    // let my_ser: syn::Path = syn::parse(
    //     format!("{}::serde::Serialize", lamellar_str)
    //         .parse()
    //         .unwrap(),
    // )
    // .unwrap();
    // let my_de: syn::Path = syn::parse(
    //     format!("{}::serde::Deserialize",lamellar_str)
    //         .parse()
    //         .unwrap(),
    // )
    // .unwrap();
    let mut impls = quote! {};
    if !local {
        impls.extend(quote! { #lamellar::serde::Serialize, #lamellar::serde::Deserialize, });
    }
    let mut trait_strs = HashMap::new();
    let mut group_trait_strs = HashMap::new();
    let mut attr_strs = HashMap::new();

    let mut create_am_group = true;

    for a in args {
        let t = a.to_token_stream().to_string();
        if t.contains("Dist") {
            trait_strs
                .entry(String::from("Dist"))
                .or_insert(quote! {#lamellar::Dist});
            trait_strs
                .entry(String::from("Copy"))
                .or_insert(quote! {Copy});
            trait_strs
                .entry(String::from("Clone"))
                .or_insert(quote! {Clone});
        } else if t.contains("ArrayOps") {
            if t.contains("(") {
                let attrs = &t[t.find("(").unwrap()
                    ..t.find(")")
                        .expect("missing \")\" in when declaring ArrayOp macro")
                        + 1];
                let attr_toks: proc_macro2::TokenStream = attrs.parse().unwrap();
                attr_strs
                    .entry(String::from("array_ops"))
                    .or_insert(quote! { #[array_ops #attr_toks]});
            }
            trait_strs
                .entry(String::from("ArrayOps"))
                .or_insert(quote! {#lamellar::ArrayOps});
            trait_strs
                .entry(String::from("Dist"))
                .or_insert(quote! {#lamellar::Dist});
            trait_strs
                .entry(String::from("Copy"))
                .or_insert(quote! {Copy});
            trait_strs
                .entry(String::from("Clone"))
                .or_insert(quote! {Clone});
        } else if t.contains("AmGroup") {
            if t.contains("(") {
                let attrs = &t[t.find("(").unwrap()
                    ..t.find(")")
                        .expect("missing \")\" in when declaring ArrayOp macro")
                        + 1];
                if attrs.contains("false") {
                    create_am_group = false;
                }
            }
        } else if !t.contains("serde::Serialize")
            && !t.contains("serde::Deserialize")
            && t.trim().len() > 0
        {
            let temp = quote::format_ident!("{}", t.trim());
            trait_strs
                .entry(t.trim().to_owned())
                .or_insert(quote! {#temp});
            if t.trim() != "Copy" {
                group_trait_strs
                    .entry(t.trim().to_owned())
                    .or_insert(quote! {#temp});
            }
        }
    }

    let mut group_impls = impls.clone();
    for t in trait_strs {
        let temp = t.1;
        impls.extend(quote! {#temp, });
    }
    for t in group_trait_strs {
        let temp = t.1;
        group_impls.extend(quote! {#temp, });
    }

    let traits = quote! { #[derive( #impls)] };
    let group_traits = quote! { #[derive( #group_impls)] };
    let serde_temp_2 = if lamellar.to_string() != "crate" && (!(local)) {
        quote! {#[serde(crate = "lamellar::serde")]}
    } else {
        quote! {}
    };
    let mut attrs = quote! {#serde_temp_2};
    for (_, attr) in attr_strs {
        attrs = quote! {
            #attrs
            #attr
        };
    }
    let group_attrs = quote! {#serde_temp_2};
    (
        traits,
        group_traits,
        attrs,
        group_attrs,
        fields,
        static_fields,
        create_am_group,
    )
}

pub(crate) fn derive_am_data(
    input: TokenStream,
    args: Punctuated<syn::Meta, syn::Token![,]>,
    lamellar: proc_macro2::TokenStream,
    local: bool,
    group: bool,
    rt: bool,
) -> TokenStream {
    let input: syn::Item = parse_macro_input!(input);
    let mut output = quote! {};

    if let syn::Item::Struct(mut data) = input {
        let name = &data.ident;
        let generics = data.generics.clone();

        let (traits, group_traits, attrs, group_attrs, fields, static_fields, create_am_group) =
            process_fields(args, &mut data.fields, &lamellar, local);

        let vis = data.vis.to_token_stream();
        let mut attributes = quote!();
        for attr in data.attrs {
            let tokens = attr.clone().to_token_stream();
            attributes.extend(quote! {#tokens});
        }

        let full_fields = quote! {
            #fields
            #static_fields
        };

        let mut full_ser = quote! {};
        // let mut full_des = quote! {};
        if !local {
            full_ser.extend(fields.ser());
            full_ser.extend(static_fields.ser());
            // full_des.extend(fields.des());
            // full_des.extend(static_fields.des());
        }

        let (orig_am, orig_am_traits) = create_am_struct(
            &generics,
            &attributes,
            &traits,
            &attrs,
            &vis,
            &name,
            &full_fields,
            &full_ser,
            // &full_des,
            &lamellar,
            local,
        );
        let (am_group_structs, am_group_traits) = if !local && !rt && !group && create_am_group {
            create_am_group_structs(
                &generics,
                &attributes,
                &group_traits,
                &group_attrs,
                &vis,
                &name,
                &fields,
                &static_fields,
                &lamellar,
                local,
            )
        } else {
            (quote! {}, quote! {})
        };

        if rt {
            output.extend(quote! {
                #orig_am
                #am_group_structs
                #orig_am_traits
                #am_group_traits
            });
        } else {
            output.extend(quote! {
                #orig_am
                #am_group_structs
                const _: () = {
                    extern crate lamellar as __lamellar;
                    use __lamellar::serde::ser::SerializeStruct;
                    #orig_am_traits
                    #am_group_traits
                };

            });
        }
    }
    TokenStream::from(output)
}
