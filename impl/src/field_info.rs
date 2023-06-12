use quote::{quote,quote_spanned,ToTokens,format_ident};
use syn::spanned::Spanned;
use syn::parse_quote;

pub(crate) struct FieldInfo{
    fields: Vec<(syn::Field,bool)>,
}

impl FieldInfo{
    pub(crate) fn new() -> Self {
        Self {
            fields: Vec::new(),
        }
    }

    pub(crate) fn add_field(&mut self, field: syn::Field,darc_iter: bool) {
        self.fields.push((field,darc_iter));
    }

    pub(crate) fn ser(&self) -> proc_macro2::TokenStream {
        let mut ser = quote!{};
        for (field,darc_iter) in &self.fields {
            match &field.ty {
                syn::Type::Path(_) => {
                    ser.extend(self.ser_path(field,*darc_iter,false));
                },
                syn::Type::Tuple(ty) => {
                    ser.extend(self.ser_tuple(field,&ty));
                }
                _ => { panic!("unsupported type in Remote Active Message {:?}", field.ty);}
            }
        }
        ser
    }

    pub(crate) fn ser_as_vecs(&self) -> proc_macro2::TokenStream {
        let mut ser = quote!{};
        for (field,darc_iter) in &self.fields {
            match &field.ty {
                syn::Type::Path(_) => {
                    ser.extend(self.ser_path(field,*darc_iter,true));
                },
                syn::Type::Tuple(ty) => {
                    ser.extend(self.ser_tuple(field,&ty));
                }
                _ => { panic!("unsupported type in Remote Active Message {:?}", field.ty);}
            }
        }
        ser
    }

    fn ser_path(&self,field: &syn::Field, darc_iter: bool,as_vecs: bool) -> proc_macro2::TokenStream {
        let field_name = field.ident.as_ref().unwrap();
        if as_vecs && darc_iter { //both
            quote!{
                for e in (&self.#field_name).iter(){
                    for d in e.iter(){
                        d.ser(num_pes,darcs);
                    }
                }
            }
        }
        else if as_vecs ^ darc_iter { //either or
            quote!{
                for e in (&self.#field_name).iter(){
                    e.ser(num_pes,darcs);
                }
            }
        }
        else { //neither
            quote!{
                (&self.#field_name).ser(num_pes,darcs);
            }
        }
    }
    

    fn ser_tuple(&self, field: &syn::Field, ty: &syn::TypeTuple) -> proc_macro2::TokenStream {
        let mut ser = quote!{};
        let field_name = field.ident.as_ref().unwrap();
        let mut ind = 0;
        for elem in &ty.elems {
            if let syn::Type::Path(ref ty) = elem {
                // if let Some(_seg) = ty.path.segments.first() {
                    let temp_ind = syn::Index {
                        index: ind,
                        span: field.span(),
                    };
                    ind += 1;
                    ser.extend(quote_spanned! {field.span()=>
                        ( &self.#field_name.#temp_ind).ser(num_pes,darcs);
                    });
                // }
            }
        }
        ser
    }

    pub(crate)fn des(&self) -> proc_macro2::TokenStream {
        let mut des = quote!{};
        for (field,darc_iter) in &self.fields {
            match &field.ty {
                syn::Type::Path(_) => {
                    des.extend(self.des_path(field,*darc_iter,false));
                },
                syn::Type::Tuple(ty) => {
                    des.extend(self.des_tuple(field,&ty));
                }
                _ => { panic!("unsupported type in Remote Active Message {:?}", field.ty);}
            }
        }
        des
    }

    pub(crate)fn des_as_vecs(&self) -> proc_macro2::TokenStream {
        let mut des = quote!{};
        for (field,darc_iter) in &self.fields {
            match &field.ty {
                syn::Type::Path(_) => {
                    des.extend(self.des_path(field,*darc_iter,true));
                },
                syn::Type::Tuple(ty) => {
                    des.extend(self.des_tuple(field,&ty));
                }
                _ => { panic!("unsupported type in Remote Active Message {:?}", field.ty);}
            }
        }
        des
    }

    fn des_path(&self,field: &syn::Field, darc_iter: bool,as_vecs: bool) -> proc_macro2::TokenStream {
        let field_name = field.ident.as_ref().unwrap();
        
        if as_vecs && darc_iter { //both
            quote!{
                for e in (&self.#field_name).iter(){
                    for d in e.iter(){
                        d.des(cur_pe);
                    }
                }
            }
        }
        else if as_vecs ^ darc_iter { //either or
            quote!{
                for e in (&self.#field_name).iter(){
                    e.des(cur_pe);
                }
            }
        }
        else { //neither
            quote!{
                (&self.#field_name).des(cur_pe);
            }
        }
    }

    fn des_tuple(&self, field: &syn::Field, ty: &syn::TypeTuple) -> proc_macro2::TokenStream {
        let mut des = quote!{};
        let field_name = field.ident.as_ref().unwrap();
        let mut ind = 0;
        for elem in &ty.elems {
            if let syn::Type::Path(ref ty) = elem {
                // if let Some(_seg) = ty.path.segments.first() {
                    let temp_ind = syn::Index {
                        index: ind,
                        span: field.span(),
                    };
                    ind += 1;
                    des.extend(quote_spanned! {field.span()=>
                        ( &self.#field_name.#temp_ind).des(cur_pe);
                    });
                // }
            }
        }
        des
    }

    pub(crate) fn names(&self) -> Vec<syn::Ident> {
        self.fields.iter().map(|(f,_)| f.ident.clone().unwrap()).collect()
    }

    pub(crate) fn types(&self) -> Vec<syn::Type> {
        self.fields.iter().map(|(f,_)| f.ty.clone()).collect()
    }
    pub(crate) fn types_as_vecs(&self) -> Vec<syn::Type> {
        self.fields.iter().map(|(f,_)| f.ty.clone()).map(|ty| parse_quote!{Vec<#ty>}).collect()
    }

    pub(crate) fn gen_vec_iter(&self) -> proc_macro2::TokenStream {
        self.fields.iter().fold(quote!{},|mut iters,(f,_)| {
            let field_name = f.ident.as_ref().unwrap();
            let field_type = &f.ty;
            let func_name = format_ident!("{}_iter",field_name);
            iters.extend(quote!{
                fn #func_name(&self) -> impl Iterator<Item = #field_type> + '_{ //} + std::slice::Iter<'_, #field_type> {
                    self.#field_name.iter().map(|e| e.clone())
                }
            });
            iters
        })
    }

    pub(crate) fn gen_getters(&self,as_ref: bool) -> proc_macro2::TokenStream {
        let (as_ref,getter) = if as_ref {
            (
                quote!{&},
                quote!{}
            )

        }
        else {
            (
                quote!{},
                quote!{[i].clone()}
            )
        };
        self.fields.iter().fold(quote!{},|mut getters,(f,_)| {
            let field_name = f.ident.as_ref().unwrap();
            let field_type = &f.ty;
            let func_name = format_ident!("{}",field_name);
            getters.extend(quote!{
                fn #func_name(&self,i: usize) -> #as_ref #field_type{ //} + std::slice::Iter<'_, #field_type> {
                    #as_ref self.#field_name #getter
                }
            });
            getters
        })
    }
    

    pub(crate) fn gen_vec_iter_types(&self) -> Vec<proc_macro2::TokenStream> { 
        self.fields.iter().map(|(f,_)| {
            let field_type = &f.ty;
            quote!{std::slice::Iter<'_, #field_type>}
        }).collect()
    }

    pub(crate) fn gen_repeat_iter(&self) -> proc_macro2::TokenStream {
        self.fields.iter().fold(quote!{},|mut iters,(f,_)| {
            let field_name = f.ident.as_ref().unwrap();
            let field_type = &f.ty;
            let func_name = format_ident!("{}_iter",field_name);
            iters.extend(quote!{
                fn #func_name(&self) -> std::iter::Repeat<&#field_type> {
                    std::iter::repeat(&self.#field_name)
                }
            });
            iters
        })
    }

    pub(crate) fn gen_repeat_iter_types(&self) -> Vec<proc_macro2::TokenStream> { 
        self.fields.iter().map(|(f,_)| {
            let field_type = &f.ty;
            quote!{std::iter::Repeat<&#field_type>}
        }).collect()
    }

    pub(crate) fn gen_iter_inits(&self) -> Vec<proc_macro2::TokenStream> {
        self.fields.iter().map(|(f,_)| {
            let field_name = f.ident.as_ref().unwrap();
            let func_name = format_ident!("{}_iter",field_name);
            quote!{self.#func_name()}
        }).collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.fields.len()
    }

    pub(crate) fn to_tokens_as_vecs(&self, )->proc_macro2::TokenStream {
        let mut tokens =quote!{};
        for (field,_) in &self.fields {
            let name = field.ident.as_ref().unwrap();
            let ty = &field.ty;
            tokens.extend(quote_spanned! {field.span()=>
                #name: Vec<#ty>,
            });
        }
        tokens
    }
}

impl ToTokens for FieldInfo {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        for (field,_) in &self.fields {
            tokens.extend(quote_spanned! {field.span()=>
                #field,
            });
        }
    }
    
}