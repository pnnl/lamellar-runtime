use syn::parse::Result;
use syn::spanned::Spanned;
use syn::visit_mut::VisitMut;
use syn::punctuated::Punctuated;
use syn::parse_quote;
// use proc_macro2::Span;
use regex::Regex;
use quote::{ quote, ToTokens};
// use proc_macro_error::proc_macro_error;
use syn::fold::{Fold,fold_expr};

use crate::parse::FormatArgs;

pub(crate) struct ReplaceSelf;

impl Fold for ReplaceSelf {
    fn fold_expr(&mut self, expr: syn::Expr) -> syn::Expr {
        let mut expr = syn::fold::fold_expr(self, expr);
        if let syn::Expr::Field(field) = expr.clone() {
            if let syn::Expr::Path(path) = field.base.as_ref() {
                if let Some(ident) = path.path.get_ident() {
                    if ident == "self" {
                        let method_call: syn::ExprMethodCall = parse_quote! {
                            #field(i)
                        };
                        expr = syn::Expr::MethodCall(method_call);
                    }
                }
            }
        }
        expr
    }
    fn fold_macro(&mut self, mac: syn::Macro) -> syn::Macro {
        let mut mac = mac.clone();
        let args: Result<FormatArgs> = mac.parse_body();
        if let Ok(args) = args {
            let format_str =  args.format_string.clone();
            let positional_args = args.positional_args.iter().map(|expr| {
                let expr = self.fold_expr(expr.clone());
                expr
            }).collect::<Vec<_>>();

            let named_args = args.named_args.iter().map(|(_name,expr)| {
                let expr = self.fold_expr(expr.clone());
                expr
            }).collect::<Vec<_>>();

            let new_tokens = quote! {
                #format_str, #(#positional_args),*, #(#named_args),*
            };
            mac.tokens=new_tokens;
        }
        else {
            println!("Warning: support for non format like macros are not currently support with AmGroups --  {:#?}", mac.to_token_stream().to_string());
            // let mac_string = mac.to_token_stream().to_string();
            // let mac_string = mac_string.replace("self", id);
            // let new_mac: syn::Macro = syn::parse_str(&mac_string).unwrap();
            // println!("{:#?}", new_mac.to_token_stream().to_string());
            // mac = new_mac;
        }
        mac
    }
}

pub(crate) struct LamellarDSLReplace;
pub(crate) struct DarcReplace;

impl VisitMut for LamellarDSLReplace {
    fn visit_ident_mut(&mut self, i: &mut syn::Ident) {
        let span = i.span();
        match i.to_string().as_str() {
            "lamellar::current_pe" => {
                *i = syn::Ident::new("__lamellar_current_pe", span);
            }
            "lamellar::num_pes" => {
                *i = syn::Ident::new("__lamellar_num_pes", span);
            }
            "lamellar::world" => {
                *i = syn::Ident::new("__lamellar_world", span);
            }
            "lamellar::team" => {
                *i = syn::Ident::new("__lamellar_team", span);
            }
            _ => {}
        }
        syn::visit_mut::visit_ident_mut(self, i);
    }
    fn visit_path_mut(&mut self, i: &mut syn::Path) {
        let span = i.span();
        // println!("seg len: {:?}", i.segments.len());
        if i.segments.len() == 2 {
            if let Some(pathseg) = i.segments.first() {
                if pathseg.ident.to_string() == "lamellar" {
                    if let Some(pathseg) = i.segments.last() {
                        match pathseg.ident.to_string().as_str() {
                            "current_pe" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new("__lamellar_current_pe", span),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            "num_pes" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new("__lamellar_num_pes", span),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            "world" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new("__lamellar_world", span),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            "team" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new("__lamellar_team", span),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        syn::visit_mut::visit_path_mut(self, i);
    }

    fn visit_macro_mut(&mut self, i: &mut syn::Macro) {
        let args: Result<FormatArgs> = i.parse_body();

        if args.is_ok() {
            let tok_str = i.tokens.to_string();
            let tok_str = tok_str.split(",").collect::<Vec<&str>>();
            let mut new_tok_str: String = tok_str[0].to_string();
            let cur_pe_re = Regex::new("lamellar(?s:.)*::(?s:.)*current_pe").unwrap();
            let num_pes_re = Regex::new("lamellar(?s:.)*::(?s:.)*num_pes").unwrap();
            let world_re = Regex::new("lamellar(?s:.)*::(?s:.)*world").unwrap();
            let team_re = Regex::new("lamellar(?s:.)*::(?s:.)*team").unwrap();
            for i in 1..tok_str.len() {
                if cur_pe_re.is_match(&tok_str[i].to_string()) {
                    new_tok_str += &(",".to_owned() + "__lamellar_current_pe");
                } else if num_pes_re.is_match(&tok_str[i].to_string()) {
                    new_tok_str += &(",".to_owned() + "__lamellar_num_pes");
                } else if world_re.is_match(&tok_str[i].to_string()) {
                    new_tok_str += &(",".to_owned() + "__lamellar_world");
                } else if team_re.is_match(&tok_str[i].to_string()) {
                    new_tok_str += &(",".to_owned() + "__lamellar_team");
                } else {
                    new_tok_str += &(",".to_owned() + &tok_str[i].to_string());
                }
            }
            // println!("new_tok_str {:?}", new_tok_str);

            i.tokens = new_tok_str.parse().unwrap();
        } else {
            // println!("warning unrecognized macro {:?} in lamellar::am expansion can currently only handle format like macros", i);
        }
        syn::visit_mut::visit_macro_mut(self, i);
    }
}

impl VisitMut for DarcReplace {
    fn visit_ident_mut(&mut self, i: &mut syn::Ident) {
        let span = i.span();
        // println!("ident: {:?}",i);
        if i.to_string() == "Darc<" {
            *i = syn::Ident::new("__AmDarc", span);
        }
        // println!("ident: {:?}",i);
        syn::visit_mut::visit_ident_mut(self, i);
    }

    fn visit_macro_mut(&mut self, i: &mut syn::Macro) {
        let args: Result<FormatArgs> = i.parse_body();

        if args.is_ok() {
            let tok_str = i.tokens.to_string();
            let tok_str = tok_str.split(",").collect::<Vec<&str>>();
            let mut new_tok_str: String = tok_str[0].to_string();
            for i in 1..tok_str.len() {
                new_tok_str +=
                    &(",".to_owned() + &tok_str[i].to_string().replace("self", "__lamellar_data"));
            }
            i.tokens = new_tok_str.parse().unwrap();
        } else {
            // println!("warning unrecognized macro {:?} in lamellar::am expansion can currently only handle format like macros", i);
        }
        syn::visit_mut::visit_macro_mut(self, i);
    }
}
