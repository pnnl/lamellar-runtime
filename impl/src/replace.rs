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


pub(crate)  fn convert_to_method_call(field: &syn::ExprField) -> syn::Expr {
    let mut ret = syn::Expr::Field(field.clone());
    if let syn::Expr::Path(path) = field.base.as_ref() {
        if let Some(ident) = path.path.get_ident() {
            if ident == "self" {
                let method_call: syn::ExprMethodCall = parse_quote! {
                    #field(i)
                };
                ret = syn::Expr::MethodCall(method_call);
            }
        }
    }
    ret
}

pub(crate) fn replace_pat_self(pat: &syn::Pat, id: &str) -> syn::Pat {
    let mut ret = pat.clone();
    match pat {
        syn::Pat::Const(_const) => {
            let mut _const = _const.clone();
            _const.block = replace_self(_const.block.clone(), id);
            ret = syn::Pat::Const(_const);
        }
        syn::Pat::Ident(ident) => { 
            let mut ident = ident.clone();
            ident.subpat = match ident.subpat {
                Some((at,subpat)) => Some((at,Box::new(replace_pat_self(&subpat, id)))),
                None => None
            };
            ret = syn::Pat::Ident(ident);
        }
        syn::Pat::Lit(_lit) => {} //do nothing
        syn::Pat::Macro(_macro) => {
            let mut _macro = _macro.clone();
            _macro.mac = replace_macro_self(&_macro.mac, id);
            ret = syn::Pat::Macro(_macro);
        }
        syn::Pat::Or(or) => {
            let mut or = or.clone();
            or.cases = or.cases.iter().fold(Punctuated::new(),|mut acc, elem| {
                acc.push(replace_pat_self(elem, id));
                acc
            });
            ret = syn::Pat::Or(or);
        }
        syn::Pat::Paren(paren) => {
            let mut paren = paren.clone();
            paren.pat = Box::new(replace_pat_self(&paren.pat, id));
            ret = syn::Pat::Paren(paren);
        }
        syn::Pat::Path(_path) => {} //do nothing if self is in path as it wont be accessing any field
        syn::Pat::Range(range) => {
            let mut range = range.clone();
            range.start = match range.start {
                Some(start) => Some(Box::new(replace_expr_self(&start, id))),
                None => None
            };
            range.end = match range.end {
                Some(end) => Some(Box::new(replace_expr_self(&end, id))),
                None => None
            };
            ret = syn::Pat::Range(range);
        }
        syn::Pat::Reference(reference) => {
            let mut reference = reference.clone();
            reference.pat = Box::new(replace_pat_self(&reference.pat, id));
        }
        syn::Pat::Rest(_rest) => {} //do nothing
        syn::Pat::Slice(slice) => {
            let mut slice = slice.clone();
            slice.elems = slice.elems.iter().fold(Punctuated::new(),|mut acc, elem| {
                acc.push(replace_pat_self(elem, id));
                acc
            });
            ret = syn::Pat::Slice(slice);
        }
        syn::Pat::Struct(_struct) => {
            let mut _struct = _struct.clone();
            _struct.fields = _struct.fields.iter().fold(Punctuated::new(),|mut acc, field| {
                let mut field = field.clone();
                field.pat = Box::new(replace_pat_self(&field.pat, id));
                acc.push(field);
                acc
            });
            ret = syn::Pat::Struct(_struct);
        }
        syn::Pat::Tuple(tuple) => {
            let mut tuple = tuple.clone();
            tuple.elems = tuple.elems.iter().fold(Punctuated::new(),|mut acc, elem| {
                acc.push(replace_pat_self(elem, id));
                acc
            });
            ret = syn::Pat::Tuple(tuple);
        }
        syn::Pat::TupleStruct(tuple_struct) => {
            let mut tuple_struct = tuple_struct.clone();
            tuple_struct.elems = tuple_struct.elems.iter().fold(Punctuated::new(),|mut acc, elem| {
                acc.push(replace_pat_self(elem, id));
                acc
            });
            ret = syn::Pat::TupleStruct(tuple_struct);
        }
        syn::Pat::Type(_type) => {} //do nothing
        syn::Pat::Verbatim(_verbatim) => {println!("unhandled pat {:?}", _verbatim)} 
        syn::Pat::Wild(_wild) => {} //do nothing

        _ => { println!("unhandled pat {:?}", pat); }
    }
    ret
}

pub(crate) fn replace_expr_self(expr: &syn::Expr, id: &str ) -> syn::Expr {
    let mut ret = expr.clone();
    match expr {
        syn::Expr::Array(array) => { 
            let mut array = array.clone();
            array.elems = array.elems.iter().fold(Punctuated::new(),|mut acc, elem| {
                acc.push(replace_expr_self(elem, id));
                acc
            });
            ret = syn::Expr::Array(array);
        }
        syn::Expr::Assign(assign) => {
            let mut assign = assign.clone();
            assign.left = Box::new(replace_expr_self(assign.left.as_ref(), id));
            assign.right = Box::new(replace_expr_self(assign.right.as_ref(), id));
            ret = syn::Expr::Assign(assign);
        }
        syn::Expr::Async(_async) => { 
            let mut _async = _async.clone();
            _async.block = replace_self(_async.block.clone(), id);
            ret = syn::Expr::Async(_async);

        }
        syn::Expr::Await(_await) => { 
            let mut _await = _await.clone();
            _await.base = Box::new(replace_expr_self(_await.base.as_ref(), id));
            ret = syn::Expr::Await(_await);
        }
        syn::Expr::Binary(binary) => {
            let mut binary = binary.clone();
            binary.left = Box::new(replace_expr_self(binary.left.as_ref(), id));
            binary.right = Box::new(replace_expr_self(binary.right.as_ref(), id));
            ret = syn::Expr::Binary(binary);
        }
        syn::Expr::Block(block) => { 
            let mut block = block.clone();
            block.block = replace_self(block.block.clone(), id);
            ret = syn::Expr::Block(block);
        }
        syn::Expr::Break(_break) => {
            let mut _break = _break.clone();
            _break.expr = match _break.expr {
                Some(expr) => Some(Box::new(replace_expr_self(expr.as_ref(), id))),
                None => None
            };
            ret = syn::Expr::Break(_break);
        }
        syn::Expr::Call(call) => {
            let mut call = call.clone();
            call.func = Box::new(replace_expr_self(call.func.as_ref(), id));
            let new_args = call.args.iter().fold(Punctuated::new(),|mut acc, arg| {
                acc.push(replace_expr_self(arg, id));
                acc
            });
            call.args = new_args;
            ret = syn::Expr::Call(call);
        }
        syn::Expr::Cast(cast) => { 
            let mut cast = cast.clone();
            cast.expr = Box::new(replace_expr_self(cast.expr.as_ref(), id));
        }
        syn::Expr::Closure(closure) => { 
            let mut closure = closure.clone();
            closure.inputs = closure.inputs.iter().fold(Punctuated::new(),|mut acc, arg| {
                acc.push(replace_pat_self(arg, id));
                acc
            });
            closure.body = Box::new(replace_expr_self(closure.body.as_ref(), id));
            ret = syn::Expr::Closure(closure);
         }
        syn::Expr::Const(_const) => { 
            let mut _const = _const.clone();
            _const.block = replace_self(_const.block.clone(), id);
            ret = syn::Expr::Const(_const);
        }
        syn::Expr::Continue(_continue) => { } //do nothing
        syn::Expr::Field(field) => {
            ret = convert_to_method_call(field);
        }
        syn::Expr::ForLoop(for_loop) => {
            let mut for_loop = for_loop.clone();
            for_loop.pat = Box::new(replace_pat_self(for_loop.pat.as_ref(), id));
            for_loop.expr = Box::new(replace_expr_self(for_loop.expr.as_ref(), id));
            for_loop.body = replace_self(for_loop.body.clone(), id);
            ret = syn::Expr::ForLoop(for_loop);
        }
        syn::Expr::Group(group) => { 
            let mut group = group.clone();
            group.expr = Box::new(replace_expr_self(group.expr.as_ref(), id));
            ret = syn::Expr::Group(group);
        }
        syn::Expr::If(_if) => { 
            let mut _if = _if.clone();
            _if.cond = Box::new(replace_expr_self(_if.cond.as_ref(), id));
            _if.then_branch = replace_self(_if.then_branch.clone(), id);
            _if.else_branch = match _if.else_branch {
                Some((_else,branch)) => Some((_else,Box::new(replace_expr_self(branch.as_ref(), id)))),
                None => None
            };
            ret = syn::Expr::If(_if);
         }
        syn::Expr::Index(index) => {
            let mut index = index.clone();
            index.expr = Box::new(replace_expr_self(index.expr.as_ref(), id));
            index.index = Box::new(replace_expr_self(index.index.as_ref(), id));
            ret = syn::Expr::Index(index);
        }
        syn::Expr::Infer(_infer) => { } //do nothing
        syn::Expr::Let(_let) => { 
            let mut _let = _let.clone();
            _let.pat = Box::new(replace_pat_self(_let.pat.as_ref(), id));
            _let.expr = Box::new(replace_expr_self(_let.expr.as_ref(), id));
            ret = syn::Expr::Let(_let);
        }
        syn::Expr::Lit(_lit) => {  } //do nothing
        syn::Expr::Loop(_loop) => {
            let mut _loop = _loop.clone();
            _loop.body = replace_self(_loop.body.clone(), id);
            ret = syn::Expr::Loop(_loop);
        }
        syn::Expr::Macro(_macro) => { 
            let mut _macro = _macro.clone();
            _macro.mac = replace_macro_self(&_macro.mac, id);
            ret = syn::Expr::Macro(_macro);
        }
        syn::Expr::Match(_match) => { 
            let mut _match = _match.clone();
            _match.expr = Box::new(replace_expr_self(_match.expr.as_ref(), id));
            _match.arms = _match.arms.iter().map(|arm| {
                let mut arm = arm.clone();
                arm.pat = replace_pat_self(&arm.pat, id);
                arm.guard = match arm.guard {
                    Some((_if,expr)) => Some((_if,Box::new(replace_expr_self(expr.as_ref(), id)))),
                    None => None
                };
                arm.body = Box::new(replace_expr_self(arm.body.as_ref(), id));
                arm
            }).collect();
            ret = syn::Expr::Match(_match);
        } 
        syn::Expr::MethodCall(call) => {
            let mut call = call.clone();
            call.receiver = Box::new(replace_expr_self( call.receiver.as_ref(), id));
            call.args =call.args.iter().fold(Punctuated::new(),|mut acc, arg| {
                acc.push(replace_expr_self(arg, id));
                acc
            });
            ret = syn::Expr::MethodCall(call);
        }
        syn::Expr::Paren(paren) => {
            let mut paren = paren.clone();
            paren.expr = Box::new(replace_expr_self(paren.expr.as_ref(), id));
            ret = syn::Expr::Paren(paren);
        }
        syn::Expr::Path(_path) => { }//do nothing
        syn::Expr::Range(range) => { 
            let mut range = range.clone();
            range.start = match range.start {
                Some(start) => Some(Box::new(replace_expr_self(start.as_ref(), id))),
                None => None
            };
            range.end = match range.end {
                Some(end) => Some(Box::new(replace_expr_self(end.as_ref(), id))),
                None => None
            };
            ret = syn::Expr::Range(range);
        }
        syn::Expr::Reference(reference) => { 
            let mut reference = reference.clone();
            reference.expr = Box::new(replace_expr_self(reference.expr.as_ref(), id));
            ret = syn::Expr::Reference(reference);
        }
        syn::Expr::Repeat(repeat) => {  
            let mut repeat = repeat.clone();
            repeat.expr = Box::new(replace_expr_self(repeat.expr.as_ref(), id));
            repeat.len = Box::new(replace_expr_self(repeat.len.as_ref(), id));
            ret = syn::Expr::Repeat(repeat);
        }
        syn::Expr::Return(_return) => { 
            let mut _return = _return.clone();
            _return.expr = match _return.expr {
                Some(expr) => Some(Box::new(replace_expr_self(expr.as_ref(), id))),
                None => None
            };
            ret = syn::Expr::Return(_return);
        }
        syn::Expr::Struct(_struct) => { 
            let mut _struct = _struct.clone();
            _struct.fields = _struct.fields.iter().fold(Punctuated::new(),|mut acc, field| {
                let mut field = field.clone();
                field.expr = replace_expr_self(&field.expr, id);
                acc.push(field);
                acc
            });
            _struct.rest = match _struct.rest {
                Some(rest) => Some(Box::new(replace_expr_self(rest.as_ref(), id))),
                None => None
            };
            ret = syn::Expr::Struct(_struct);
        }
        syn::Expr::Try(_try) => { 
            let mut _try = _try.clone();
            _try.expr = Box::new(replace_expr_self(_try.expr.as_ref(), id));
            ret = syn::Expr::Try(_try);
        }
        syn::Expr::TryBlock(try_block) => { 
            let mut try_block = try_block.clone();
            try_block.block = replace_self(try_block.block.clone(), id);
            ret = syn::Expr::TryBlock(try_block);
        }
        syn::Expr::Tuple(tuple) => { 
            let mut tuple = tuple.clone();
            tuple.elems = tuple.elems.iter().fold(Punctuated::new(),|mut acc, elem| {
                acc.push(replace_expr_self(elem, id));
                acc
            });
            ret = syn::Expr::Tuple(tuple);
        }
        syn::Expr::Unary(unary) => {  
            let mut unary = unary.clone();
            unary.expr = Box::new(replace_expr_self(unary.expr.as_ref(), id));
            ret = syn::Expr::Unary(unary);
        }
        syn::Expr::Unsafe(_unsafe) => { 
            let mut _unsafe = _unsafe.clone();
            _unsafe.block = replace_self(_unsafe.block.clone(), id);
            ret = syn::Expr::Unsafe(_unsafe);
        }
        syn::Expr::Verbatim(_verbatim) => { println!("unhandled expr {:?}", _verbatim) }
        syn::Expr::While(_while) => {
            let mut _while = _while.clone();
            _while.cond = Box::new(replace_expr_self(_while.cond.as_ref(), id));
            _while.body = replace_self(_while.body.clone(), id);
            ret = syn::Expr::While(_while);
        }
        syn::Expr::Yield(_yield) => { 
            let mut _yield = _yield.clone();
            _yield.expr = match _yield.expr {
                Some(expr) => Some(Box::new(replace_expr_self(expr.as_ref(), id))),
                None => None
            };
            ret = syn::Expr::Yield(_yield);
        }

        _ => { println!("Unhandled expr {:?}", expr); }
    }
    ret
}

pub(crate) fn replace_macro_self(mac: &syn::Macro, id: &str) -> syn::Macro {
    let mut mac = mac.clone();
    let args: Result<FormatArgs> = mac.parse_body();
    // println!("{:#?}", args);
    if let Ok(args) = args {
        let format_str =  args.format_string.clone();
        let positional_args = args.positional_args.iter().map(|expr| {
            let expr = replace_expr_self(expr, id);
            expr
        }).collect::<Vec<_>>();

        let named_args = args.named_args.iter().map(|(_name,expr)| {
            let expr = replace_expr_self(expr, id);
            expr
        }).collect::<Vec<_>>();

        let new_tokens = quote! {
            #format_str, #(#positional_args),*, #(#named_args),*
        };
        mac.tokens=new_tokens;
    }
    else {
        println!("Warning: support for non format like macros are experimental --  {:#?}", mac.to_token_stream().to_string());
        let mac_string = mac.to_token_stream().to_string();
        let mac_string = mac_string.replace("self", id);
        let new_mac: syn::Macro = syn::parse_str(&mac_string).unwrap();
        println!("{:#?}", new_mac.to_token_stream().to_string());
        mac = new_mac;
    }
    mac
}

pub(crate) fn replace_self(fn_block: syn::Block, id: &str) -> syn::Block {
    let mut new_block = fn_block.clone();
    for stmt in &mut new_block.stmts {
        match stmt {
            syn::Stmt::Local(local) => {
                // println!("{:#?}", local.init);
                if let Some(init) = &mut local.init {
                    init.expr = Box::new(replace_expr_self(init.expr.as_ref(), id));
                }
                // println!("{:#?}", local.init);
            }
            syn::Stmt::Item(item) => {
                panic!(
                    "Error unexpected item in lamellar am {}",
                    item.to_token_stream().to_string()
                );
            }
            syn::Stmt::Expr(expr, semi) => {
                // println!("{:#?}", expr);
                let new_expr = replace_expr_self(expr, id);
                
                *stmt = syn::Stmt::Expr(new_expr, semi.clone());
                
            }
            syn::Stmt::Macro(mac) => {
                mac.mac = replace_macro_self(&mac.mac, id);
            }
        }
    }
    new_block
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
