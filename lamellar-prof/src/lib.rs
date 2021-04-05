#![cfg_attr(
    not(feature = "enable-prof"),
    allow(dead_code),
    allow(unused_variables),
    allow(unused_mut)
)]
extern crate proc_macro;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
// use syn::parse::{Parse, ParseStream, Result};
use syn::{parse_macro_input, parse_quote};

fn type_name(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Path(syn::TypePath { qself: None, path }) => {
            Some(path.segments.last().unwrap().ident.to_string())
        }
        _ => {
            // println!("{:?}",ty);
            println!("warning! lamellar-prof only instruments impl for concrete non generic types");
            None
        }
    }
}

fn instrument_block(name: String, input: &mut syn::Block, generics: &std::vec::Vec<String>) {
    let name = syn::LitStr::new(&name, Span::call_site());
    if generics.len() > 0 {
        let mut new_stmts = vec![];
        let temp: syn::Stmt = parse_quote! {
             let mut _lamellar_prof_name = #name.to_owned()+"<";
        };
        new_stmts.push(temp);
        for ty in generics {
            let temp: syn::Stmt = syn::parse_str(&format!(
                "_lamellar_prof_name += &(\"{}=\".to_owned()+std::any::type_name::<{}>()+\",\");",
                ty, ty
            ))
            .unwrap();
            new_stmts.push(temp);
        }
        let temp: syn::Stmt = parse_quote! { _lamellar_prof_name+=">";};
        new_stmts.push(temp);
        let temp: syn::Stmt = parse_quote! {
            let _lamellar_prof_instance = crate::timer_start(_lamellar_prof_name);
        };
        new_stmts.push(temp);
        while let Some(stmt) = new_stmts.pop() {
            input.stmts.insert(0, stmt);
        }
    } else {
        let stmt: syn::Stmt = parse_quote! {
            let _lamellar_prof_instance = crate::timer_start( #name.to_owned() );
        };
        input.stmts.insert(0, stmt);
    }
}

fn get_generics_list(input: &syn::Generics) -> std::vec::Vec<String> {
    let mut names = vec![];
    for param in &input.params {
        // if let syn::TypeParam { attrs, ident, colon_token, bounds, eq_token, default } = param  {
        if let syn::GenericParam::Type(ty) = param {
            println!("{:?}", ty.ident.to_string());
            names.push(ty.ident.to_string());
        }
    }
    names
}

fn instrument_fn(input: &mut syn::ItemFn) {
    //-> proc_macro2::TokenStream {
    // let name = type_name(&input.self_ty).expect("unable to find function name");
    // println!("function name: {}", input.sig.ident.to_string());
    // let mut input = input.clone();

    let generics = get_generics_list(&input.sig.generics);
    instrument_block(input.sig.ident.to_string(), &mut input.block, &generics);

    // let output = quote! { #input };
    // output
}

fn instrument_impl(input: &mut syn::ItemImpl) {
    //-> proc_macro2::TokenStream {
    // let mut input = input.clone();

    if let Some(name) = type_name(&input.self_ty) {
        let generics = get_generics_list(&input.generics);
        for item in &mut input.items {
            match item {
                syn::ImplItem::Method(ref mut item) => {
                    let func_name = name.clone() + "::" + &item.sig.ident.to_string();
                    println!("function name: {}", func_name);
                    instrument_block(func_name, &mut item.block, &generics);
                }
                _ => (),
            }
        }
    }
    // let output = quote! { #input };
    // output
}

#[proc_macro_attribute]
pub fn prof(_args: TokenStream, items: TokenStream) -> TokenStream {
    let mut input: syn::Item = parse_macro_input!(items);
    #[cfg(feature = "enable-prof")]
    {
        let input = &mut input;

        // let output =
        match input {
            syn::Item::Impl(input) => instrument_impl(input),
            syn::Item::Fn(input) => instrument_fn(input),
            _ => {
                println!("lamellar::prof attribute only valid for functions or impl blocks");
                // let output = quote! { #input };
                // output
            }
        }
        // output
    }
    let output = quote! { #input };
    output.into()
}

#[proc_macro]
pub fn init_prof(_item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    #[cfg(feature = "enable-prof")]
    {
        // let output = &mut output;
        output.extend( quote!{
            thread_local! {
                pub (crate) static TIMER: std::cell::RefCell<MyTimer> = std::cell::RefCell::new(
                    MyTimer{
                        top_level_calls: std::collections::HashMap::new(),
                        stack: std::vec::Vec::new(),
                        trace: std::vec::Vec::new(),
                });
            }

            pub(crate) fn timer_start(func: String) -> TimeInst{
                // println!("start: {}",func.clone());
                let func_clone = func.clone();
                TIMER.with(|timer| {
                    timer.borrow_mut().start(func_clone);
                });
                TimeInst {func: func}
            }

            pub(crate) fn timer_stop(func: String){
                // println!("stop: {}",func.clone());
                TIMER.with(|timer| {
                    timer.borrow_mut().stop(func);
                })
            }

            pub(crate) fn timer_print(){
                TIMER.with(|timer| {
                    let mut timer = timer.borrow_mut();
                    timer.convert_trace();

                    let mut func_calls = timer.top_level_calls.values().collect::<Vec<_>>();

                    func_calls.sort_by(|a, b| b.partial_cmp(a).unwrap());
                    let mut print_string: String = "".to_string();
                    for func in func_calls{
                        print_string += &func.borrow().print("".to_string());
                    }
                    println!("{}",print_string);
                })
            }

            pub(crate) struct TimeInst {
                func: String,
            }
            impl Drop for TimeInst{
                fn drop(&mut self) {
                    timer_stop(self.func.clone());
                    // println!("dropped: {}",self.func);
                }
            }

            #[derive(Debug)]
            pub (crate) struct FuncCall{
                name: String,
                count: usize,
                time: f64,
                instant: std::time::Instant,
                sub_calls: std::collections::HashMap<String,std::rc::Rc<std::cell::RefCell<FuncCall>>>,
            }

            impl FuncCall{
                fn print(&self,prefix: String) -> String {
                    let mut print_string = format!("[{:?}]{}{}: {} {}\n",std::thread::current().id(),prefix.clone(),self.name,self.count,self.time);
                    let mut func_calls = self.sub_calls.values().collect::<Vec<_>>();
                    func_calls.sort_by(|a, b| b.partial_cmp(a).unwrap());
                    for func in func_calls{
                        print_string += &func.borrow().print(prefix.clone()+"  ");
                    }
                    print_string
                }
            }

            impl PartialOrd for FuncCall{
                fn partial_cmp(&self, other: &FuncCall) -> Option<std::cmp::Ordering> {
                    self.time.partial_cmp(&other.time)
                }
            }
            impl PartialEq for FuncCall{
                fn eq(&self, other: &FuncCall) -> bool{
                    self.time == other.time
                }
            }

            pub(crate) struct MyTimer{
                top_level_calls: std::collections::HashMap<String,std::rc::Rc<std::cell::RefCell<FuncCall>>>,
                stack: std::vec::Vec<std::rc::Rc<std::cell::RefCell<FuncCall>>>,
                trace: std::vec::Vec<(String,bool,std::time::Instant)>,
            }

            impl MyTimer{
                fn start(&mut self, func: String){
                    // let mut func_call = if let Some(mut parent_call) = self.stack.last_mut(){
                    //     let mut parent_call = parent_call.borrow_mut();
                    //     parent_call.sub_calls.entry(func.clone()).or_insert(std::rc::Rc::new(std::cell::RefCell::new(FuncCall{
                    //         name: func.clone(),
                    //         count: 0,
                    //         time: 0.0,
                    //         instant: std::time::Instant::now(),
                    //         sub_calls: std::collections::HashMap::new(),
                    //     }))).clone()
                    // }
                    // else{
                    //     self.top_level_calls.entry(func.clone()).or_insert(std::rc::Rc::new(std::cell::RefCell::new(FuncCall{
                    //         name: func.clone(),
                    //         count: 0,
                    //         time: 0.0,
                    //         instant: std::time::Instant::now(),
                    //         sub_calls: std::collections::HashMap::new(),
                    //     }))).clone()
                    // };
                    // self.stack.push(func_call.clone());
                    // let mut mut_func_call = func_call.borrow_mut();
                    // mut_func_call.count+=1;
                    // mut_func_call.instant = std::time::Instant::now();
                    self.trace.push((func,true,std::time::Instant::now()));
                }

                fn stop(&mut self, func: String){
                    // if let Some(mut func_call) =  self.stack.pop(){
                    //     let mut func_call = func_call.borrow_mut();
                    //     func_call.time += func_call.instant.elapsed().as_secs_f64();
                    //     // println!("{} {:?}",func,func_call.instant.elapsed().as_secs_f64());
                    // }
                    self.trace.push((func,false,std::time::Instant::now()));
                }

                fn convert_trace(&mut self){
                    for (func,begin,instant) in self.trace.clone(){
                        // println!("{:?} {:?} ",func.clone(),begin);
                        if begin {
                            self.prof_start(func.clone(),instant.clone());
                        }
                        else{
                            self.prof_end(func.clone(),instant.clone());
                        }
                    }
                }

                fn prof_start(&mut self,func: String,start: std::time::Instant){
                    let mut func_call = if let Some(mut parent_call) = self.stack.last_mut(){
                        let mut parent_call = parent_call.borrow_mut();
                        parent_call.sub_calls.entry(func.clone()).or_insert(std::rc::Rc::new(std::cell::RefCell::new(FuncCall{
                            name: func.clone(),
                            count: 0,
                            time: 0.0,
                            instant: start,
                            sub_calls: std::collections::HashMap::new(),
                        }))).clone()
                    }
                    else{
                        self.top_level_calls.entry(func.clone()).or_insert(std::rc::Rc::new(std::cell::RefCell::new(FuncCall{
                            name: func.clone(),
                            count: 0,
                            time: 0.0,
                            instant: start,
                            sub_calls: std::collections::HashMap::new(),
                        }))).clone()
                    };
                    self.stack.push(func_call.clone());
                    let mut mut_func_call = func_call.borrow_mut();
                    mut_func_call.count+=1;
                    mut_func_call.instant = start;
                }

                fn prof_end(&mut self, func: String, end: std::time::Instant){
                    if let Some(mut func_call) =  self.stack.pop(){
                        let mut func_call = func_call.borrow_mut();
                        func_call.time += (end - func_call.instant).as_secs_f64();
                        // println!("{} {:?}",func,func_call.instant.elapsed().as_secs_f64());
                    }
                }
            }
        });
    }
    output.into()
}

#[proc_macro]
pub fn fini_prof(_item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    #[cfg(feature = "enable-prof")]
    {
        // output = &mut output;
        output.extend(quote! {
            crate::timer_print();
        });
    }
    output.into()
}

#[proc_macro]
pub fn prof_start(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    #[cfg(feature = "enable-prof")]
    {
        let name = item.to_string();
        let code: syn::Stmt = syn::parse_str(&format!(
            "let _lamellar_prof_instance{} = crate::timer_start( {:?}.to_string() );",
            name, name
        ))
        .unwrap();

        output.extend(quote! {#code});
    }
    output.into()
}

#[proc_macro]
pub fn prof_end(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    #[cfg(feature = "enable-prof")]
    {
        let name = item.to_string();
        let code: syn::Stmt =
            syn::parse_str(&format!("drop(_lamellar_prof_instance{});", name)).unwrap();
        output.extend(quote! { #code });
    }
    output.into()
}
