use quote::{format_ident, quote, quote_spanned, ToTokens};
use syn::fold::Fold;
use syn::parse::Result;
use syn::spanned::Spanned;
// use syn::visit_mut::VisitMut;
use syn::{parse_quote, parse_quote_spanned};

use crate::parse::{FormatArgs, VecArgs};

pub(crate) struct ReplaceSelf {
    am_name: syn::Ident,
    pub(crate) accessed_fields: std::collections::HashMap<syn::Ident, proc_macro2::TokenStream>,
}

impl ReplaceSelf {
    pub(crate) fn new(am_name: syn::Ident) -> Self {
        Self {
            am_name,
            accessed_fields: std::collections::HashMap::new(),
        }
    }
}

impl Fold for ReplaceSelf {
    fn fold_expr(&mut self, expr: syn::Expr) -> syn::Expr {
        let mut expr = syn::fold::fold_expr(self, expr);
        if let syn::Expr::Field(field) = expr.clone() {
            if let syn::Expr::Path(path) = field.base.as_ref() {
                if let Some(ident) = path.path.get_ident() {
                    if ident == "self" {
                        let field_member = field.member.clone();
                        let new_field_member = format_ident!("__{}", field_member);
                        self.accessed_fields.insert(
                            new_field_member.clone(),
                            quote_spanned! {field.span()=> let #new_field_member = #field(i);},
                        );
                        // let method_call: syn::ExprMethodCall = parse_quote! {
                        //     #field(i)
                        // };
                        let path: syn::ExprPath = parse_quote!(#new_field_member);
                        expr = syn::Expr::Path(path);
                    }
                }
            }
        }
        expr
    }
    fn fold_macro(&mut self, mac: syn::Macro) -> syn::Macro {
        let mut mac = mac.clone();
        let args: Result<FormatArgs> = mac.clone().parse_body();
        let span = mac.span();
        if let Ok(args) = args {
            let format_str = args.format_string.clone();
            let positional_args =
                args.positional_args
                    .iter()
                    .fold(quote_spanned!(span=>), |acc, expr| {
                        let expr = self.fold_expr(expr.clone());
                        quote!(#acc , #expr)
                    });
            let named_args =
                args.named_args
                    .iter()
                    .fold(quote_spanned!(span=>), |acc, (_name, expr)| {
                        let expr = self.fold_expr(expr.clone());
                        quote!(#acc , #expr)
                    });

            mac.tokens = quote_spanned! {mac.span()=>
                #format_str #positional_args #named_args
            };
            return mac;
        }
        let args: Result<VecArgs> = mac.clone().parse_body();
        if let Ok(args) = args {
            mac.tokens = match args {
                VecArgs::List(args) => args.iter().fold(quote_spanned!(span=>), |acc, expr| {
                    let expr = self.fold_expr(expr.clone());
                    quote!(#acc , #expr)
                }),
                VecArgs::Size((elem, size)) => {
                    let elem = self.fold_expr(elem.clone());
                    let size = self.fold_expr(size.clone());
                    quote!(#elem; #size)
                }
            };
            return mac;
        } else {
            let mac_string = mac.to_token_stream().to_string();
            if mac_string.contains("self.") {
                println!("Warning: support for non format like macros are currently experimental with AmGroups ({:?} appears in {:?})", mac.clone().to_token_stream().to_string(),self.am_name.to_string());
                println!("Below are a few workarounds that may work:");
                println!("1. assign \"self.<field>\" to a local variable, and use that variable in the macro instead");
                println!("2. If your active message fails to compile, first ensure it compiles when AmGroups are disabled by adding AmGroup(false) the at AmData macro, e.g. #[AmData(AmGroup(false))] on the LamellarAM attribute macro, e.g. #[am(AmGroup(false))]");
                println!("  2.1 If you are able to successfully compile with AmGroups disabled, please open an issue at https://github.com/pnnl/lamellar-runtime/issues and include the following in the description: {:#?}", mac.to_token_stream().to_string());

                let mac_string = mac_string.replace("self.", "__");
                let new_mac: syn::Macro = syn::parse_str(&mac_string).unwrap();
                // println!("{:#?}", new_mac.to_token_stream().to_string());
                mac = new_mac;
            }
        }
        mac
    }
}

pub(crate) struct LamellarDSLReplace;
// pub(crate) struct DarcReplace;

impl Fold for LamellarDSLReplace {
    fn fold_expr_path(&mut self, path: syn::ExprPath) -> syn::ExprPath {
        let mut path = syn::fold::fold_expr_path(self, path);
        let span = path.span();
        let path_str = path
            .path
            .to_token_stream()
            .to_string()
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>();

        match path_str.as_str() {
            "lamellar::current_pe" => path = parse_quote_spanned!(span=> __lamellar_current_pe),
            "lamellar::num_pes" => path = parse_quote_spanned!(span=> __lamellar_num_pes),
            "lamellar::world" => path = parse_quote_spanned!(span=> __lamellar_world),
            "lamellar::team" => path = parse_quote_spanned!(span=> __lamellar_team),
            "lamellar::tid" => path = parse_quote_spanned!(span=> __lamellar_thread_id),
            _ => {}
        }
        path
    }
    fn fold_macro(&mut self, mac: syn::Macro) -> syn::Macro {
        let mut mac = syn::fold::fold_macro(self, mac);
        let span = mac.span();
        let args: Result<FormatArgs> = mac.clone().parse_body();
        if let Ok(args) = args {
            let format_str = args.format_string.clone();
            let positional_args =
                args.positional_args
                    .iter()
                    .fold(quote_spanned!(span=>), |acc, expr| {
                        let expr = self.fold_expr(expr.clone());
                        quote!(#acc , #expr)
                    });
            let named_args =
                args.named_args
                    .iter()
                    .fold(quote_spanned!(span=>), |acc, (_name, expr)| {
                        let expr = self.fold_expr(expr.clone());
                        quote!(#acc , #expr)
                    });

            mac.tokens = quote_spanned! {mac.span()=>
                #format_str #positional_args #named_args
            };
            return mac;
        }

        let args: Result<VecArgs> = mac.clone().parse_body();
        if let Ok(args) = args {
            mac.tokens = match args {
                VecArgs::List(args) => args.iter().fold(quote_spanned!(span=>), |acc, expr| {
                    let expr = self.fold_expr(expr.clone());
                    quote!(#acc , #expr)
                }),
                VecArgs::Size((elem, size)) => {
                    let elem = self.fold_expr(elem.clone());
                    let size = self.fold_expr(size.clone());
                    quote!(#elem; #size)
                }
            };
            return mac;
        } else {
            //println!("Warning: support for non format like macros are not currently support with AmGroups --  {:#?}", mac.to_token_stream().to_string());

            let mac_string = mac.to_token_stream().to_string();
            let mac_string = mac_string.replace("lamellar::current_pe", "__lamellar_current_pe");
            let mac_string = mac_string.replace("lamellar::num_pes", "__lamellar_num_pes");
            let mac_string = mac_string.replace("lamellar::world", "__lamellar_world");
            let mac_string = mac_string.replace("lamellar::team", "__lamellar_team");
            let mac_string =
                mac_string.replace("lamellar::tid", "__lamellar_thread_id");
            let mut new_mac: syn::Macro = syn::parse_str(&mac_string).unwrap();
            new_mac = parse_quote_spanned!(span=> #new_mac);
            // println!("{:#?}", new_mac.to_token_stream().to_string());
            mac = new_mac;
        }
        mac
    }
}

// impl VisitMut for DarcReplace {
//     fn visit_ident_mut(&mut self, i: &mut syn::Ident) {
//         let span = i.span();
//         // println!("ident: {:?}",i);
//         if i.to_string() == "Darc<" {
//             *i = syn::Ident::new("__AmDarc", span);
//         }
//         // println!("ident: {:?}",i);
//         syn::visit_mut::visit_ident_mut(self, i);
//     }

//     fn visit_macro_mut(&mut self, i: &mut syn::Macro) {
//         let args: Result<FormatArgs> = i.parse_body();

//         if args.is_ok() {
//             let tok_str = i.tokens.to_string();
//             let tok_str = tok_str.split(",").collect::<Vec<&str>>();
//             let mut new_tok_str: String = tok_str[0].to_string();
//             for i in 1..tok_str.len() {
//                 new_tok_str +=
//                     &(",".to_owned() + &tok_str[i].to_string().replace("self", "__lamellar_data"));
//             }
//             i.tokens = new_tok_str.parse().unwrap();
//         } else {
//             // println!("warning unrecognized macro {:?} in lamellar::am expansion can currently only handle format like macros", i);
//         }
//         syn::visit_mut::visit_macro_mut(self, i);
//     }
// }
