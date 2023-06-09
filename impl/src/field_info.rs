use quote::{quote,quote_spanned,ToTokens};
use syn::spanned::Spanned;

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
                    ser.extend(self.ser_path(field,*darc_iter));
                },
                syn::Type::Tuple(ty) => {
                    ser.extend(self.ser_tuple(field,&ty));
                }
                _ => { panic!("unsupported type in Remote Active Message {:?}", field.ty);}
            }
        }
        ser
    }

    fn ser_path(&self, field: &syn::Field, darc_iter: bool) -> proc_macro2::TokenStream {
        let field_name = field.ident.as_ref().unwrap();
        if darc_iter {
            quote!{
                for e in (&self.#field_name).iter(){
                    e.ser(num_pes,darcs);
                }
            }
        }
        else{
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
                    des.extend(self.des_path(field,*darc_iter));
                },
                syn::Type::Tuple(ty) => {
                    des.extend(self.des_tuple(field,&ty));
                }
                _ => { panic!("unsupported type in Remote Active Message {:?}", field.ty);}
            }
        }
        des
    }

    fn des_path(&self,field: &syn::Field, darc_iter: bool) -> proc_macro2::TokenStream {
        let field_name = field.ident.as_ref().unwrap();
        if darc_iter {
            quote!{
                for e in (&self.#field_name).iter(){
                    e.des(cur_pe);
                }
            }
        }
        else{
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

    pub(crate) fn len(&self) -> usize {
        self.fields.len()
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