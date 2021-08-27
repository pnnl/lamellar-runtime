use syn::visit_mut::VisitMut;
use syn::parse::Result;
use syn::spanned::Spanned;
// use proc_macro2::Span;
use regex::Regex;
// use proc_macro_error::proc_macro_error;

use crate::parse::{FormatArgs};

pub(crate) struct SelfReplace;
pub(crate) struct LamellarDSLReplace;
pub(crate) struct DarcReplace;

impl VisitMut for SelfReplace {
    fn visit_ident_mut(&mut self, i: &mut syn::Ident) {
        let span = i.span();
        // println!("ident: {:?}",i);
        if i.to_string() == "self" {
            *i = syn::Ident::new("__lamellar_data", span);
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

impl VisitMut for LamellarDSLReplace {
    fn visit_ident_mut(&mut self, i: &mut syn::Ident) {
        let span = i.span();
        match i.to_string().as_str() {
            "lamellar::current_pe" => {
                *i = syn::Ident::new("__lamellar_current_pe", span);
            }
            "lamellar::num_pes" => {
                *i = syn::Ident::new("__lamellar_num_pes",  span);
            }
            "lamellar::world" => {
                *i = syn::Ident::new("__lamellar_world",  span);
            }
            "lamellar::team" => {
                *i = syn::Ident::new("__lamellar_team",  span);
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
                                    ident: syn::Ident::new(
                                        "__lamellar_current_pe",
                                        span,
                                    ),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            "num_pes" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new("__lamellar_num_pes",  span),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            "world" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new("__lamellar_world",  span),
                                    arguments: syn::PathArguments::None,
                                });
                            }
                            "team" => {
                                (*i).segments = syn::punctuated::Punctuated::new();
                                (*i).segments.push(syn::PathSegment {
                                    ident: syn::Ident::new("__lamellar_team",  span),
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
            *i = syn::Ident::new("__AmDarc",  span);
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

