use proc_macro_error::{abort, abort_call_site};
use syn::ext::IdentExt;
use syn::parse::{Parse, ParseStream, Result};
use syn::{Expr, ExprClosure, Ident, Token, TypePath};

#[derive(Debug)]
pub(crate) struct FormatArgs {
    pub(crate) _format_string: Expr,
    pub(crate) _positional_args: Vec<Expr>,
    pub(crate) _named_args: Vec<(Ident, Expr)>,
}

impl Parse for FormatArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let _format_string: Expr;
        let mut _positional_args = Vec::new();
        let mut _named_args = Vec::new();

        _format_string = input.parse()?;
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            if input.peek(Ident::peek_any) && input.peek2(Token![=]) {
                while !input.is_empty() {
                    let name: Ident = input.call(Ident::parse_any)?;
                    input.parse::<Token![=]>()?;
                    let value: Expr = input.parse()?;
                    _named_args.push((name, value));
                    if input.is_empty() {
                        break;
                    }
                    input.parse::<Token![,]>()?;
                }
                break;
            }
            _positional_args.push(input.parse()?);
        }

        Ok(FormatArgs {
            _format_string,
            _positional_args,
            _named_args,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ReductionArgs {
    pub(crate) name: Ident,
    pub(crate) closure: ExprClosure,
    pub(crate) tys: Vec<TypePath>,
}

impl Parse for ReductionArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut tys = vec![];
        let name = match input.parse() {
            Ok(name) => name,
            Err(_) => abort_call_site!("register reduction expects a name as first argument"),
        };
        input.parse::<Token![,]>()?;
        let closure = match input.parse() {
            Ok(closure) => closure,
            Err(_) => abort_call_site!(
                "register reduction expects a closure of form: |a,b| {...} as second argument"
            ),
        };
        if !input.peek(Token![,]) {
            abort_call_site!("register reduction requires registering for at least one type");
        }
        while !input.is_empty() {
            let comma = input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            tys.push(match input.parse() {
                Ok(ty) => ty,
                Err(_e) => abort!(
                    comma,
                    "register reduction requires registering for at least one type"
                ),
            })
        }
        Ok(ReductionArgs {
            name: name,
            closure: closure,
            tys: tys,
        })
    }
}
