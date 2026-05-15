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
use syn::{parse_macro_input, parse_quote};

fn type_name(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Path(syn::TypePath { qself: None, path }) => {
            Some(path.segments.last().unwrap().ident.to_string())
        }
        _ => {
            println!("warning! lamellar-prof only instruments impl for concrete non-generic types");
            None
        }
    }
}

fn get_generics_list(input: &syn::Generics) -> Vec<String> {
    input
        .params
        .iter()
        .filter_map(|param| {
            if let syn::GenericParam::Type(ty) = param {
                Some(ty.ident.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn instrument_block(name: String, input: &mut syn::Block, generics: &[String]) {
    let name_lit = syn::LitStr::new(&name, Span::call_site());

    // When use-lamellar is active, route through ::lamellar::prof so both runtime
    // and application code share a single timer. Otherwise use crate:: for standalone.
    #[cfg(feature = "use-lamellar")]
    let timer_start_tokens = quote! { ::lamellar::prof::timer_start };
    #[cfg(not(feature = "use-lamellar"))]
    let timer_start_tokens = quote! { crate::timer_start };

    if generics.is_empty() {
        // Non-generic: name is a 'static str literal — no heap allocation in hot path.
        let stmt: syn::Stmt = parse_quote! {
            let _lamellar_prof_instance = #timer_start_tokens(#name_lit);
        };
        input.stmts.insert(0, stmt);
    } else {
        // Generic: build the name string once per call, borrow it into timer_start.
        let mut new_stmts: Vec<syn::Stmt> = vec![];
        new_stmts.push(parse_quote! {
            let mut _lamellar_prof_name = #name_lit.to_owned() + "<";
        });
        for ty in generics {
            new_stmts.push(
                syn::parse_str(&format!(
                    "_lamellar_prof_name += &(\"{}=\".to_owned() + std::any::type_name::<{}>() + \",\");",
                    ty, ty
                ))
                .unwrap(),
            );
        }
        new_stmts.push(parse_quote! { _lamellar_prof_name += ">"; });
        new_stmts.push(parse_quote! {
            let _lamellar_prof_instance = #timer_start_tokens(&_lamellar_prof_name);
        });
        while let Some(stmt) = new_stmts.pop() {
            input.stmts.insert(0, stmt);
        }
    }
}

// For async functions: replaces the block body with
//   ProfFuture::new(name, async { <original body> }).await
// This preserves the `async fn` signature while correctly tracking
// active-poll time vs suspended-await time via ProfFuture's poll/drop hooks.
fn instrument_block_async(name: String, input: &mut syn::Block, generics: &[String]) {
    let name_lit = syn::LitStr::new(&name, Span::call_site());
    let original_stmts = std::mem::take(&mut input.stmts);

    #[cfg(feature = "use-lamellar")]
    let prof_future_tokens = quote! { ::lamellar::prof::ProfFuture::new };
    #[cfg(not(feature = "use-lamellar"))]
    let prof_future_tokens = quote! { crate::ProfFuture::new };

    // Build the wrapping expression via quote! and parse as Expr (not Stmt) to
    // avoid parse_quote! choking on a bare `.await` expression without semicolon.
    if generics.is_empty() {
        let tokens = quote! {
            #prof_future_tokens(#name_lit, async { #(#original_stmts)* }).await
        };
        let expr: syn::Expr = syn::parse2(tokens)
            .unwrap_or_else(|e| panic!("instrument_block_async: {}", e));
        input.stmts = vec![syn::Stmt::Expr(expr)];
    } else {
        let mut new_stmts: Vec<syn::Stmt> = vec![];
        new_stmts.push(parse_quote! {
            let mut _lamellar_prof_name = #name_lit.to_owned() + "<";
        });
        for ty in generics {
            new_stmts.push(
                syn::parse_str(&format!(
                    "_lamellar_prof_name += &(\"{}=\".to_owned() + std::any::type_name::<{}>() + \",\");",
                    ty, ty
                ))
                .unwrap(),
            );
        }
        new_stmts.push(parse_quote! { _lamellar_prof_name += ">"; });
        let tokens = quote! {
            #prof_future_tokens(&_lamellar_prof_name, async { #(#original_stmts)* }).await
        };
        let expr: syn::Expr = syn::parse2(tokens)
            .unwrap_or_else(|e| panic!("instrument_block_async: {}", e));
        new_stmts.push(syn::Stmt::Expr(expr));
        input.stmts = new_stmts;
    }
}

fn instrument_fn(input: &mut syn::ItemFn) {
    let generics = get_generics_list(&input.sig.generics);
    if input.sig.asyncness.is_some() {
        instrument_block_async(input.sig.ident.to_string(), &mut input.block, &generics);
    } else {
        instrument_block(input.sig.ident.to_string(), &mut input.block, &generics);
    }
}

fn instrument_impl(input: &mut syn::ItemImpl) {
    if let Some(name) = type_name(&input.self_ty) {
        let generics = get_generics_list(&input.generics);
        for item in &mut input.items {
            if let syn::ImplItem::Method(method) = item {
                let func_name = format!("{}::{}", name, method.sig.ident);
                if method.sig.asyncness.is_some() {
                    instrument_block_async(func_name, &mut method.block, &generics);
                } else {
                    instrument_block(func_name, &mut method.block, &generics);
                }
            }
        }
    }
}

/// Instrument a function or all methods in an impl block for profiling.
///
/// Place on an individual `fn` to time that function, or on an `impl` block to
/// time every method in it. Call `init_prof!()` once at your crate root and
/// `fini_prof!()` to print results. Only active when the `enable-prof` feature
/// is enabled; zero overhead otherwise.
#[proc_macro_attribute]
pub fn prof(_args: TokenStream, items: TokenStream) -> TokenStream {
    let mut input: syn::Item = parse_macro_input!(items);
    #[cfg(feature = "enable-prof")]
    {
        match &mut input {
            syn::Item::Impl(input) => instrument_impl(input),
            syn::Item::Fn(input) => instrument_fn(input),
            _ => println!("lamellar::prof attribute is only valid on functions or impl blocks"),
        }
    }
    quote! { #input }.into()
}

/// Instrument all methods in an impl block for profiling.
///
/// Preferred over `#[prof]` when annotating impl blocks — the name makes the
/// intent explicit. Also accepts a single `fn` for convenience.
/// Requires `init_prof!()` at the crate root. Only active when `enable-prof`
/// is enabled; zero overhead otherwise.
#[proc_macro_attribute]
pub fn prof_all(_args: TokenStream, items: TokenStream) -> TokenStream {
    let mut input: syn::Item = parse_macro_input!(items);
    #[cfg(feature = "enable-prof")]
    {
        match &mut input {
            syn::Item::Impl(input) => instrument_impl(input),
            syn::Item::Fn(input) => instrument_fn(input),
            _ => println!("lamellar::prof_all attribute is only valid on impl blocks or functions"),
        }
    }
    quote! { #input }.into()
}

/// Emit RAII-stack profiling infrastructure into the calling crate.
///
/// Call once at your crate root (typically before any `#[prof]`-annotated code
/// is reached). This macro generates the `TIMER` thread-local, `timer_start`,
/// `ProfFuture`, and supporting types so that `#[prof]` / `#[prof_all]`
/// expansions (`crate::timer_start`, `crate::ProfFuture::new`) resolve
/// correctly in your crate.
///
/// Only generates code when the `enable-prof` feature is active.
#[proc_macro]
pub fn init_prof(_item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    #[cfg(feature = "enable-prof")]
    {
        output.extend(quote! {
            thread_local! {
                pub(crate) static TIMER: std::cell::RefCell<MyTimer> = std::cell::RefCell::new(
                    MyTimer {
                        top_level_calls: std::collections::HashMap::new(),
                        stack: Vec::new(),
                    }
                );
            }

            pub(crate) fn timer_start(func: &str) -> TimeInst {
                TIMER.with(|t| t.borrow_mut().start(func));
                TimeInst
            }

            pub(crate) fn timer_start_first(
                func: &str,
            ) -> (TimeInst, std::rc::Rc<std::cell::RefCell<FuncCall>>) {
                let fc = TIMER.with(|t| {
                    let mut timer = t.borrow_mut();
                    let child = timer.get_or_create(func);
                    {
                        let mut fc = child.borrow_mut();
                        fc.count += 1;
                        fc.start = std::time::Instant::now();
                    }
                    timer.stack.push(child.clone());
                    child
                });
                (TimeInst, fc)
            }

            pub(crate) fn timer_resume(
                fc: &std::rc::Rc<std::cell::RefCell<FuncCall>>,
            ) -> TimeInst {
                TIMER.with(|t| {
                    let mut timer = t.borrow_mut();
                    fc.borrow_mut().start = std::time::Instant::now();
                    timer.stack.push(fc.clone());
                });
                TimeInst
            }

            pub(crate) fn timer_stop() {
                TIMER.with(|t| t.borrow_mut().stop());
            }

            pub(crate) fn timer_print() {
                TIMER.with(|t| {
                    let timer = t.borrow();
                    let mut calls: Vec<_> = timer.top_level_calls.values().collect();
                    calls.sort_by(|a, b| b.partial_cmp(a).unwrap());
                    let mut out = String::new();
                    for f in calls {
                        out += &f.borrow().print(String::new());
                    }
                    println!("{}", out);
                });
            }

            pub(crate) struct TimeInst;

            impl Drop for TimeInst {
                fn drop(&mut self) {
                    timer_stop();
                }
            }

            #[derive(Debug)]
            pub(crate) struct FuncCall {
                name: String,
                count: usize,
                time: f64,
                await_time: f64,
                start: std::time::Instant,
                sub_calls: std::collections::HashMap<
                    String,
                    std::rc::Rc<std::cell::RefCell<FuncCall>>,
                >,
            }

            impl FuncCall {
                fn new(name: &str) -> Self {
                    FuncCall {
                        name: name.to_owned(),
                        count: 0,
                        time: 0.0,
                        await_time: 0.0,
                        start: std::time::Instant::now(),
                        sub_calls: std::collections::HashMap::new(),
                    }
                }

                fn print(&self, prefix: String) -> String {
                    let time_str = if self.await_time > 0.0 {
                        format!(
                            "active={:.6}s await={:.6}s",
                            self.time, self.await_time,
                        )
                    } else {
                        format!("time={:.6}s", self.time)
                    };
                    let mut out = format!(
                        "{}{}: count={} {}\n",
                        prefix,
                        self.name,
                        self.count,
                        time_str,
                    );
                    let mut sub: Vec<_> = self.sub_calls.values().collect();
                    sub.sort_by(|a, b| b.partial_cmp(a).unwrap());
                    for f in sub {
                        out += &f.borrow().print(format!("{}  ", prefix));
                    }
                    out
                }
            }

            impl PartialOrd for FuncCall {
                fn partial_cmp(&self, other: &FuncCall) -> Option<std::cmp::Ordering> {
                    self.time.partial_cmp(&other.time)
                }
            }

            impl PartialEq for FuncCall {
                fn eq(&self, other: &FuncCall) -> bool {
                    self.time == other.time
                }
            }

            pub(crate) struct MyTimer {
                top_level_calls: std::collections::HashMap<
                    String,
                    std::rc::Rc<std::cell::RefCell<FuncCall>>,
                >,
                stack: Vec<std::rc::Rc<std::cell::RefCell<FuncCall>>>,
            }

            impl MyTimer {
                fn get_or_create(
                    &mut self,
                    func: &str,
                ) -> std::rc::Rc<std::cell::RefCell<FuncCall>> {
                    if let Some(parent_rc) = self.stack.last() {
                        let mut parent = parent_rc.borrow_mut();
                        if let Some(rc) = parent.sub_calls.get(func) {
                            rc.clone()
                        } else {
                            parent
                                .sub_calls
                                .entry(func.to_owned())
                                .or_insert_with(|| {
                                    std::rc::Rc::new(std::cell::RefCell::new(FuncCall::new(func)))
                                })
                                .clone()
                        }
                    } else if let Some(rc) = self.top_level_calls.get(func) {
                        rc.clone()
                    } else {
                        self.top_level_calls
                            .entry(func.to_owned())
                            .or_insert_with(|| {
                                std::rc::Rc::new(std::cell::RefCell::new(FuncCall::new(func)))
                            })
                            .clone()
                    }
                }

                fn start(&mut self, func: &str) {
                    let child = self.get_or_create(func);
                    self.stack.push(child.clone());
                    let mut fc = child.borrow_mut();
                    fc.count += 1;
                    fc.start = std::time::Instant::now();
                }

                fn stop(&mut self) {
                    if let Some(fc_rc) = self.stack.pop() {
                        let start = fc_rc.borrow().start;
                        fc_rc.borrow_mut().time += start.elapsed().as_secs_f64();
                    }
                }
            }

            pub(crate) struct ProfFuture<F: std::future::Future> {
                inner: F,
                name: String,
                func_call: Option<std::rc::Rc<std::cell::RefCell<FuncCall>>>,
                last_suspend: Option<std::time::Instant>,
            }

            impl<F: std::future::Future> ProfFuture<F> {
                pub(crate) fn new(name: &str, inner: F) -> Self {
                    ProfFuture {
                        inner,
                        name: name.to_owned(),
                        func_call: None,
                        last_suspend: None,
                    }
                }
            }

            impl<F: std::future::Future> std::future::Future for ProfFuture<F> {
                type Output = F::Output;

                fn poll(
                    self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Self::Output> {
                    let this = unsafe { self.get_unchecked_mut() };

                    if let Some(suspend) = this.last_suspend.take() {
                        if let Some(fc) = &this.func_call {
                            fc.borrow_mut().await_time += suspend.elapsed().as_secs_f64();
                        }
                    }

                    let _guard = if let Some(fc) = &this.func_call {
                        timer_resume(fc)
                    } else {
                        let (guard, fc) = timer_start_first(&this.name);
                        this.func_call = Some(fc);
                        guard
                    };

                    let result = unsafe { std::pin::Pin::new_unchecked(&mut this.inner) }.poll(cx);

                    drop(_guard);

                    match result {
                        std::task::Poll::Pending => {
                            this.last_suspend = Some(std::time::Instant::now());
                            std::task::Poll::Pending
                        }
                        std::task::Poll::Ready(v) => std::task::Poll::Ready(v),
                    }
                }
            }

            impl<F: std::future::Future> Drop for ProfFuture<F> {
                fn drop(&mut self) {
                    if let (Some(suspend), Some(fc)) = (self.last_suspend.take(), &self.func_call) {
                        fc.borrow_mut().await_time += suspend.elapsed().as_secs_f64();
                    }
                }
            }
        });
    }
    output.into()
}

/// Emit backtrace-based profiling infrastructure into the calling crate.
///
/// Like `init_prof!()` but uses `backtrace::trace` to capture call stacks,
/// enabling per-call-site attribution in the printed output. Uses a global
/// `thread_local::ThreadLocal` so that `timer_print()` aggregates data from
/// all threads (worker threads, async executors, etc.).
///
/// Requires `backtrace` and `thread_local` crates as dependencies of the
/// calling crate.
///
/// Only generates code when the `backtrace-prof` feature is active AND
/// `use-lamellar` is NOT active (when `use-lamellar` is on, the infrastructure
/// lives in `lamellar::prof` directly — no codegen needed).
#[proc_macro]
pub fn init_prof_bt(_item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    // When use-lamellar is active, the timer infrastructure is already compiled
    // into lamellar/src/prof.rs — emitting it again would produce duplicate items.
    #[cfg(all(feature = "backtrace-prof", not(feature = "use-lamellar")))]
    {
        output.extend(quote! {
            // UnsafeCell wraps ThreadLocal so iter_mut() can be called at print
            // time without the &T→&mut T UB lint. Sound because fini_prof!() is
            // called after all threads join, satisfying iter_mut() exclusivity.
            struct _ProfTimerStatic(
                std::cell::UnsafeCell<thread_local::ThreadLocal<std::cell::RefCell<MyTimer>>>
            );
            unsafe impl Sync for _ProfTimerStatic {}

            pub(crate) static TIMER: _ProfTimerStatic =
                _ProfTimerStatic(std::cell::UnsafeCell::new(thread_local::ThreadLocal::new()));

            fn _prof_timer() -> std::cell::RefMut<'static, MyTimer> {
                unsafe { &*TIMER.0.get() }.get_or(|| std::cell::RefCell::new(
                    MyTimer {
                        calls: std::collections::HashMap::new(),
                        ip_cache: std::collections::HashMap::new(),
                    }
                )).borrow_mut()
            }

            pub(crate) fn timer_start(func: &str) -> TimeInst {
                let ips = {
                    let mut timer = _prof_timer();
                    if let Some(cached) = timer.ip_cache.get(func) {
                        cached.clone()
                    } else {
                        let mut ips = Vec::with_capacity(32);
                        backtrace::trace(|frame| {
                            ips.push(frame.ip() as usize);
                            ips.len() < 64
                        });
                        timer.ip_cache.insert(func.to_owned(), ips.clone());
                        ips
                    }
                };
                TimeInst { ips, func: func.to_owned(), start: std::time::Instant::now() }
            }

            pub(crate) fn timer_print() {
                // Safety: called after all worker threads have completed (e.g., after
                // world.barrier() + wait_all()), so no thread is currently accessing
                // its cell — iter_mut() exclusivity invariant is satisfied.
                let tl_mut = unsafe { &mut *TIMER.0.get() };
                let raw_entries: Vec<(Vec<usize>, CallRecord)> = tl_mut.iter_mut()
                    .flat_map(|cell| {
                        let t = cell.get_mut();
                        t.calls.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>()
                    })
                    .collect();

                let mut merged: std::collections::HashMap<Vec<String>, CallRecord> =
                    std::collections::HashMap::new();
                for (ips, record) in &raw_entries {
                    let path = MyTimer::resolve_path(ips, &record.func_name);
                    let e = merged.entry(path).or_insert_with(|| CallRecord {
                        count: 0,
                        total_time: 0.0,
                        await_time: 0.0,
                        func_name: record.func_name.clone(),
                    });
                    e.count += record.count;
                    e.total_time += record.total_time;
                    e.await_time += record.await_time;
                }
                let mut entries: Vec<(Vec<String>, CallRecord)> = merged.into_iter().collect();
                entries.sort_by(|a, b| {
                    b.1.total_time.partial_cmp(&a.1.total_time)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                let has_shallow = entries.iter().any(|(p, _)| p.len() == 1);
                let mut root_prefixes: Vec<Vec<String>> = {
                    let mut set = std::collections::HashSet::new();
                    if has_shallow { set.insert(vec![]); }
                    for (p, _) in &entries {
                        if p.len() <= 1 { continue; }
                        let parent = p[..p.len() - 1].to_vec();
                        if !entries.iter().any(|(q, _)| q == &parent) {
                            set.insert(parent);
                        }
                    }
                    let mut v: Vec<Vec<String>> = set.into_iter().collect();
                    v.sort_by(|a, b| {
                        let max_time = |pfx: &Vec<String>| {
                            entries.iter()
                                .filter(|(p, _)| p.starts_with(pfx.as_slice()))
                                .map(|(_, r)| r.total_time)
                                .fold(0.0f64, f64::max)
                        };
                        max_time(b).partial_cmp(&max_time(a))
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    v
                };
                if root_prefixes.is_empty() {
                    print!("{}", MyTimer::print_subtree(&entries, &[], 0));
                } else {
                    for prefix in &root_prefixes {
                        print!("{}", MyTimer::print_subtree(&entries, prefix, 0));
                    }
                }
            }

            pub(crate) struct TimeInst {
                ips: Vec<usize>,
                func: String,
                start: std::time::Instant,
            }

            impl Drop for TimeInst {
                fn drop(&mut self) {
                    let ips = std::mem::take(&mut self.ips);
                    let func = std::mem::take(&mut self.func);
                    let elapsed = self.start.elapsed().as_secs_f64();
                    let mut timer = _prof_timer();
                    let record = timer.calls.entry(ips).or_insert_with(|| CallRecord {
                        count: 0,
                        total_time: 0.0,
                        await_time: 0.0,
                        func_name: func,
                    });
                    record.count += 1;
                    record.total_time += elapsed;
                }
            }

            #[derive(Clone, Debug)]
            pub(crate) struct CallRecord {
                count: usize,
                total_time: f64,
                await_time: f64,
                func_name: String,
            }

            pub(crate) struct MyTimer {
                calls: std::collections::HashMap<Vec<usize>, CallRecord>,
                // Caches the captured IP sequence for each function name so that
                // backtrace::trace() (which acquires a global unwind lock on Linux)
                // is only called once per function per thread rather than on every
                // invocation.  Thread-local placement means no cross-thread contention.
                ip_cache: std::collections::HashMap<String, Vec<usize>>,
            }

            impl MyTimer {
                fn is_noise(sym: &str) -> bool {
                    const NOISE: &[&str] = &[
                        "std::", "core::", "alloc::",
                        "<std::", "<core::", "<alloc::",
                        "backtrace::", "addr2line::", "gimli::",
                        "rustc_demangle::", "memchr::", "object::",
                        "miniz_oxide::",
                        "futures_executor::", "futures_task::", "futures_util::",
                        "tokio::", "smol::", "async_std::",
                    ];
                    NOISE.iter().any(|p| sym.starts_with(p))
                        || sym.ends_with("::timer_start")
                        || sym.contains("::ProfFuture::")
                        || sym.contains("{{closure}}")
                        || matches!(sym, "_start" | "__libc_start_main" | "main")
                        || sym.starts_with("::")
                }

                fn strip_generics(sym: &str) -> String {
                    let mut out = String::with_capacity(sym.len());
                    let mut depth = 0usize;
                    for c in sym.chars() {
                        match c {
                            '<' => depth += 1,
                            '>' => { depth = depth.saturating_sub(1); }
                            _ if depth == 0 => out.push(c),
                            _ => {}
                        }
                    }
                    let mut clean = String::with_capacity(out.len());
                    let mut colon_run = 0u8;
                    for c in out.chars() {
                        if c == ':' { colon_run += 1; }
                        else {
                            if colon_run > 0 { clean.push_str("::"); colon_run = 0; }
                            clean.push(c);
                        }
                    }
                    if colon_run > 0 { clean.push_str("::"); }
                    clean
                }

                fn strip_hash(sym: &str) -> &str {
                    if let Some(pos) = sym.rfind("::h") {
                        if sym[pos + 3..].chars().all(|c| c.is_ascii_hexdigit()) {
                            return &sym[..pos];
                        }
                    }
                    sym
                }

                fn strip_closure_suffix(sym: &str) -> &str {
                    let mut s = sym;
                    while let Some(stripped) = s.strip_suffix("::{{closure}}") {
                        s = stripped;
                    }
                    s
                }

                pub fn resolve_path(ips: &[usize], func_name: &str) -> Vec<String> {
                    let current_base = func_name.split('<').next().unwrap_or(func_name);
                    let mut raw: Vec<String> = Vec::new();
                    let mut func_sym: Option<String> = None;

                    for &ip in ips {
                        let mut sym_str: Option<String> = None;
                        backtrace::resolve(ip as *mut _, |sym| {
                            if sym_str.is_some() { return; }
                            if let Some(name) = sym.name() {
                                sym_str = Some(format!("{}", name));
                            }
                        });
                        let sym = match sym_str { Some(s) => s, None => continue };
                        let clean = Self::strip_generics(
                            Self::strip_closure_suffix(Self::strip_hash(&sym))
                        );
                        if Self::is_noise(&clean) { continue; }
                        if func_sym.is_none()
                            && (clean == current_base
                                || clean.ends_with(&format!("::{}", current_base)))
                        {
                            func_sym = Some(clean);
                            continue;
                        }
                        raw.push(clean);
                    }

                    raw.reverse();
                    raw.dedup();
                    let mut path = raw;
                    path.push(func_sym.unwrap_or_else(|| func_name.to_owned()));
                    path
                }

                pub fn print_subtree(
                    entries: &[(Vec<String>, CallRecord)],
                    prefix: &[String],
                    depth: usize,
                ) -> String {
                    let mut children: Vec<&(Vec<String>, CallRecord)> = entries
                        .iter()
                        .filter(|(p, _)| {
                            if !p.starts_with(prefix) || p.len() <= prefix.len() {
                                return false;
                            }
                            if prefix.is_empty() {
                                return p.len() == 1;
                            }
                            !entries.iter().any(|(q, _)| {
                                q.as_slice() != p.as_slice()
                                    && q.starts_with(prefix)
                                    && q.len() > prefix.len()
                                    && p.starts_with(q.as_slice())
                                    && q.len() < p.len()
                            })
                        })
                        .collect();
                    children.sort_by(|a, b| {
                        b.1.total_time.partial_cmp(&a.1.total_time)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });

                    let indent = "  ".repeat(depth);
                    let mut out = String::new();
                    for (path, record) in &children {
                        let time_str = if record.await_time > 0.0 {
                            format!(
                                "active={:.6}s await={:.6}s",
                                record.total_time, record.await_time,
                            )
                        } else {
                            format!("time={:.6}s", record.total_time)
                        };
                        let path_str = if depth == 0 {
                            path.join("::")
                        } else {
                            path[prefix.len()..].iter()
                                .fold(String::new(), |acc, s| acc + "::" + s)
                        };
                        out += &format!(
                            "{}{}: count={} {}\n",
                            indent, path_str,
                            record.count, time_str,
                        );
                        out += &MyTimer::print_subtree(entries, path, depth + 1);
                    }
                    out
                }
            }

            pub(crate) struct ProfFuture<F: std::future::Future> {
                inner: F,
                func: String,
                ips: Vec<usize>,
                first_poll: bool,
                active_time: f64,
                await_time: f64,
                last_suspend: Option<std::time::Instant>,
            }

            impl<F: std::future::Future> ProfFuture<F> {
                pub(crate) fn new(name: &str, inner: F) -> Self {
                    ProfFuture {
                        inner,
                        func: name.to_owned(),
                        ips: Vec::new(),
                        first_poll: true,
                        active_time: 0.0,
                        await_time: 0.0,
                        last_suspend: None,
                    }
                }

                fn flush_to_timer(&mut self) {
                    if self.ips.is_empty() { return; }
                    let ips = std::mem::take(&mut self.ips);
                    let func = std::mem::take(&mut self.func);
                    let active = self.active_time;
                    let awaiting = self.await_time;
                    let mut timer = _prof_timer();
                    let record = timer.calls.entry(ips).or_insert_with(|| CallRecord {
                        count: 0,
                        total_time: 0.0,
                        await_time: 0.0,
                        func_name: func,
                    });
                    record.count += 1;
                    record.total_time += active;
                    record.await_time += awaiting;
                }
            }

            impl<F: std::future::Future> std::future::Future for ProfFuture<F> {
                type Output = F::Output;

                fn poll(
                    self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Self::Output> {
                    let this = unsafe { self.get_unchecked_mut() };

                    let poll_start = std::time::Instant::now();

                    if let Some(suspend) = this.last_suspend.take() {
                        this.await_time += poll_start.duration_since(suspend).as_secs_f64();
                    }

                    if this.first_poll {
                        this.first_poll = false;
                        let mut timer = _prof_timer();
                        this.ips = if let Some(cached) = timer.ip_cache.get(&this.func) {
                            cached.clone()
                        } else {
                            let mut ips = Vec::with_capacity(32);
                            backtrace::trace(|frame| {
                                ips.push(frame.ip() as usize);
                                ips.len() < 64
                            });
                            timer.ip_cache.insert(this.func.clone(), ips.clone());
                            ips
                        };
                    }

                    let result =
                        unsafe { std::pin::Pin::new_unchecked(&mut this.inner) }.poll(cx);

                    this.active_time += poll_start.elapsed().as_secs_f64();

                    match result {
                        std::task::Poll::Pending => {
                            this.last_suspend = Some(std::time::Instant::now());
                            std::task::Poll::Pending
                        }
                        std::task::Poll::Ready(v) => {
                            this.flush_to_timer();
                            std::task::Poll::Ready(v)
                        }
                    }
                }
            }

            impl<F: std::future::Future> Drop for ProfFuture<F> {
                fn drop(&mut self) {
                    if let Some(suspend) = self.last_suspend.take() {
                        self.await_time += suspend.elapsed().as_secs_f64();
                    }
                    self.flush_to_timer();
                }
            }
        });
    }
    output.into()
}

/// Print the accumulated profiling data for the current thread and reset state.
/// Call at the end of the region you want to profile (e.g. end of `main`).
/// Only active when `enable-prof` is enabled.
#[proc_macro]
pub fn fini_prof(_item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    #[cfg(feature = "enable-prof")]
    {
        // When use-lamellar is active, the timer lives in lamellar::prof.
        // Otherwise (standalone use), it lives in crate:: (generated by init_prof_bt!).
        #[cfg(feature = "use-lamellar")]
        output.extend(quote! { ::lamellar::prof::timer_print(); });
        #[cfg(not(feature = "use-lamellar"))]
        output.extend(quote! { crate::timer_print(); });
    }
    output.into()
}

/// Manually start timing a named region within a function.
///
/// The timer stops automatically when the binding goes out of scope (RAII).
/// Pair with `prof_end!(name)` to stop early. Only active when `enable-prof`
/// is enabled.
///
/// # Example
/// ```rust,ignore
/// prof_start!(my_section);
/// // ... work ...
/// prof_end!(my_section);
/// ```
#[proc_macro]
pub fn prof_start(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    #[cfg(feature = "enable-prof")]
    {
        let name = item.to_string();
        let name_lit = syn::LitStr::new(&name, Span::call_site());
        let binding: syn::Ident =
            syn::parse_str(&format!("_lamellar_prof_instance_{}", name)).unwrap();
        #[cfg(feature = "use-lamellar")]
        let timer_start_tokens = quote! { ::lamellar::prof::timer_start };
        #[cfg(not(feature = "use-lamellar"))]
        let timer_start_tokens = quote! { crate::timer_start };
        let code: syn::Stmt = parse_quote! {
            let #binding = #timer_start_tokens(#name_lit);
        };
        output.extend(quote! { #code });
    }
    output.into()
}

/// Manually stop a named region started with `prof_start!(name)`.
///
/// Drops the `TimeInst` binding created by `prof_start!`, recording elapsed time.
/// Only active when `enable-prof` is enabled.
#[proc_macro]
pub fn prof_end(item: TokenStream) -> TokenStream {
    let mut output = quote! {};
    #[cfg(feature = "enable-prof")]
    {
        let name = item.to_string();
        let binding: syn::Ident =
            syn::parse_str(&format!("_lamellar_prof_instance_{}", name)).unwrap();
        let code: syn::Stmt = parse_quote! { drop(#binding); };
        output.extend(quote! { #code });
    }
    output.into()
}
