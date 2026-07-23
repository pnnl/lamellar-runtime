use std::cell::{RefCell, RefMut, UnsafeCell};
use std::collections::HashMap;
use thread_local::ThreadLocal;

// Newtype so we can impl Sync: UnsafeCell<T> is !Sync but we need a static.
// Safety invariant: get_or accesses only the calling thread's slot (ThreadLocal
// per-thread isolation). iter_mut in timer_print is only called after all worker
// threads have finished — the exclusivity required by &mut is upheld.
struct TimerStatic(UnsafeCell<ThreadLocal<RefCell<MyTimer>>>);
unsafe impl Sync for TimerStatic {}
static TIMER: TimerStatic = TimerStatic(UnsafeCell::new(ThreadLocal::new()));

pub(crate) fn _prof_timer() -> RefMut<'static, MyTimer> {
    unsafe { &*TIMER.0.get() }
        .get_or(|| {
            RefCell::new(MyTimer {
                calls: HashMap::new(),
                ip_cache: HashMap::new(),
            })
        })
        .borrow_mut()
}

/// Starts timing a call to `func`, capturing a backtrace to build its call path.
/// Records the elapsed time when the returned [`TimeInst`] is dropped.
pub fn timer_start(func: &str) -> TimeInst {
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
    TimeInst {
        ips,
        func: func.to_owned(),
        start: std::time::Instant::now(),
    }
}

// Merges per-thread raw entries into a single path map, populating thread_times
// so that print_subtree can show per-thread breakdown inline.
fn merge_entries(per_thread: &[Vec<(Vec<usize>, CallRecord)>]) -> Vec<(Vec<String>, CallRecord)> {
    // First pass per thread: resolve IPs to paths, accumulate per-thread totals.
    let thread_maps: Vec<HashMap<Vec<String>, (usize, f64, f64)>> = per_thread
        .iter()
        .map(|entries| {
            let mut m: HashMap<Vec<String>, (usize, f64, f64)> = HashMap::new();
            for (ips, record) in entries {
                let path = MyTimer::resolve_path(ips, &record.func_name);
                let e = m.entry(path).or_insert((0, 0.0, 0.0));
                e.0 += record.count;
                e.1 += record.total_time;
                e.2 += record.await_time;
            }
            m
        })
        .collect();

    // Collect all known paths.
    let mut all_paths: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
    for tm in &thread_maps {
        for k in tm.keys() {
            all_paths.insert(k.clone());
        }
    }

    // Build merged entries with per-thread time vectors.
    let mut merged: Vec<(Vec<String>, CallRecord)> = all_paths
        .into_iter()
        .map(|path| {
            let mut count = 0usize;
            let mut total_time = 0.0f64;
            let mut await_time = 0.0f64;
            let mut func_name = String::new();
            let mut thread_times: Vec<f64> = Vec::with_capacity(thread_maps.len());

            for tm in &thread_maps {
                if let Some(&(c, t, a)) = tm.get(&path) {
                    count += c;
                    total_time += t;
                    await_time += a;
                    thread_times.push(t);
                    if func_name.is_empty() {
                        func_name = path.last().cloned().unwrap_or_default();
                    }
                } else {
                    thread_times.push(0.0);
                }
            }

            (
                path,
                CallRecord {
                    count,
                    total_time,
                    await_time,
                    func_name,
                    thread_times,
                },
            )
        })
        .collect();

    merged.sort_by(|a, b| {
        b.1.total_time
            .partial_cmp(&a.1.total_time)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    merged
}

fn print_tree(entries: &[(Vec<String>, CallRecord)]) -> String {
    // Build root_prefixes from entries with len > 1 whose immediate parent isn't
    // in entries.  Entries with len == 1 have an empty parent — those are handled
    // by an explicit [] prefix rather than added here, so they don't collide with
    // deeper entries that also match a [] root and would otherwise print twice.
    let has_shallow = entries.iter().any(|(p, _)| p.len() == 1);

    let root_prefixes: Vec<Vec<String>> = {
        let mut set = std::collections::HashSet::new();
        // Empty prefix for depth-1 entries (only when they exist).
        if has_shallow {
            set.insert(vec![]);
        }
        for (p, _) in entries {
            if p.len() <= 1 {
                continue; // depth-1 entries handled via [] above
            }
            let parent = p[..p.len() - 1].to_vec();
            if !entries.iter().any(|(q, _)| q == &parent) {
                set.insert(parent);
            }
        }
        let mut v: Vec<Vec<String>> = set.into_iter().collect();
        v.sort_by(|a, b| {
            let max_time = |pfx: &Vec<String>| {
                entries
                    .iter()
                    .filter(|(p, _)| p.starts_with(pfx.as_slice()))
                    .map(|(_, r)| r.total_time)
                    .fold(0.0f64, f64::max)
            };
            max_time(b)
                .partial_cmp(&max_time(a))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        v
    };

    let mut out = String::new();
    if root_prefixes.is_empty() {
        // All entries have len > 1 and their parents are in entries; no explicit
        // root — fall back to printing everything from the empty prefix.
        out += &MyTimer::print_subtree(entries, &[], 0);
    } else {
        for prefix in &root_prefixes {
            out += &MyTimer::print_subtree(entries, prefix, 0);
        }
    }
    out
}

/// Merges all threads' recorded call timings and prints them as a call tree.
/// Call after all worker threads have finished.
pub fn timer_print() {
    // Safety: called after all worker threads have completed (e.g., after world drops),
    // so no thread is currently accessing its cell — iter_mut() exclusivity holds.
    let tl_mut = unsafe { &mut *TIMER.0.get() };

    let per_thread: Vec<Vec<(Vec<usize>, CallRecord)>> = tl_mut
        .iter_mut()
        .map(|cell| {
            let t = cell.get_mut();
            t.calls
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .collect();

    if per_thread.is_empty() {
        return;
    }

    let entries = merge_entries(&per_thread);
    print!("{}", print_tree(&entries));
}

/// RAII guard returned by [`timer_start`] that records the call's elapsed time on drop.
pub struct TimeInst {
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
            thread_times: Vec::new(),
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
    // Populated during merge: one element per thread that contributed to this entry.
    thread_times: Vec<f64>,
}

pub(crate) struct MyTimer {
    calls: HashMap<Vec<usize>, CallRecord>,
    // Caches IP sequence per function name so backtrace::trace() (global unwind lock
    // on Linux) is only called once per (thread, function) pair, not every invocation.
    ip_cache: HashMap<String, Vec<usize>>,
}

impl MyTimer {
    fn is_noise(sym: &str) -> bool {
        const NOISE: &[&str] = &[
            "std::",
            "core::",
            "alloc::",
            "<std::",
            "<core::",
            "<alloc::",
            "backtrace::",
            "addr2line::",
            "gimli::",
            "rustc_demangle::",
            "memchr::",
            "object::",
            "miniz_oxide::",
            "futures_executor::",
            "futures_task::",
            "futures_util::",
            "tokio::",
            "smol::",
            "async_std::",
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
                '>' => {
                    depth = depth.saturating_sub(1);
                }
                _ if depth == 0 => out.push(c),
                _ => {}
            }
        }
        let mut clean = String::with_capacity(out.len());
        let mut colon_run = 0u8;
        for c in out.chars() {
            if c == ':' {
                colon_run += 1;
            } else {
                if colon_run > 0 {
                    clean.push_str("::");
                    colon_run = 0;
                }
                clean.push(c);
            }
        }
        if colon_run > 0 {
            clean.push_str("::");
        }
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

    fn resolve_path(ips: &[usize], func_name: &str) -> Vec<String> {
        let current_base = func_name.split('<').next().unwrap_or(func_name);
        let mut raw: Vec<String> = Vec::new();
        let mut func_sym: Option<String> = None;

        for &ip in ips {
            let mut sym_str: Option<String> = None;
            backtrace::resolve(ip as *mut _, |sym| {
                if sym_str.is_some() {
                    return;
                }
                if let Some(name) = sym.name() {
                    sym_str = Some(format!("{}", name));
                }
            });
            let sym = match sym_str {
                Some(s) => s,
                None => continue,
            };
            let clean = Self::strip_generics(Self::strip_closure_suffix(Self::strip_hash(&sym)));
            if Self::is_noise(&clean) {
                continue;
            }
            if func_sym.is_none()
                && (clean == current_base || clean.ends_with(&format!("::{}", current_base)))
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

    fn print_subtree(
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
                    // With an empty prefix, only show depth-1 entries here.
                    // Deeper entries appear under their own non-empty root_prefix
                    // subtrees and must not also appear at depth 0 (would duplicate).
                    return p.len() == 1;
                }
                // With a non-empty prefix, show direct children: entries that have
                // no intermediate ancestor between this prefix and themselves.
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
            b.1.total_time
                .partial_cmp(&a.1.total_time)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let indent = "  ".repeat(depth);
        let n_threads = children.first().map_or(0, |(_, r)| r.thread_times.len());
        let mut out = String::new();
        for (path, record) in &children {
            // Cumulative time portion.
            let time_str = if record.await_time > 0.0 {
                format!(
                    "active={:.6}s await={:.6}s",
                    record.total_time, record.await_time,
                )
            } else {
                format!("time={:.6}s", record.total_time)
            };

            // Per-thread breakdown + average (only when more than one thread contributed).
            let thread_str = if n_threads > 1 && !record.thread_times.is_empty() {
                let non_zero: Vec<f64> = record
                    .thread_times
                    .iter()
                    .copied()
                    .filter(|&t| t > 0.0)
                    .collect();
                if non_zero.len() > 1 {
                    let avg = non_zero.iter().sum::<f64>() / non_zero.len() as f64;
                    let times: Vec<String> = record
                        .thread_times
                        .iter()
                        .map(|t| format!("{:.6}s", t))
                        .collect();
                    format!(" [threads: {} | avg: {:.6}s]", times.join(", "), avg)
                } else {
                    String::new()
                }
            } else {
                String::new()
            };

            let path_str = if depth == 0 {
                path.join("::")
            } else {
                path[prefix.len()..]
                    .iter()
                    .fold(String::new(), |acc, s| acc + "::" + s)
            };
            out += &format!(
                "{}{}: count={} {}{}\n",
                indent, path_str, record.count, time_str, thread_str,
            );
            out += &MyTimer::print_subtree(entries, path, depth + 1);
        }
        out
    }
}

/// Wraps a [`Future`](std::future::Future), tracking active/await time across polls
/// and recording it to the profiler timer when the future completes or is dropped.
pub struct ProfFuture<F: std::future::Future> {
    inner: F,
    func: String,
    ips: Vec<usize>,
    first_poll: bool,
    active_time: f64,
    await_time: f64,
    last_suspend: Option<std::time::Instant>,
}

impl<F: std::future::Future> ProfFuture<F> {
    /// Wraps `inner`, profiling it under the given call `name` once polled.
    pub fn new(name: &str, inner: F) -> Self {
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
        if self.ips.is_empty() {
            return;
        }
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
            thread_times: Vec::new(),
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

        let result = unsafe { std::pin::Pin::new_unchecked(&mut this.inner) }.poll(cx);

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
