use std::sync::atomic::{AtomicBool, Ordering};

use crate::config;

static ENABLED: AtomicBool = AtomicBool::new(true);

pub(crate) enum RuntimeWarning<'a> {
    UnspawnedTask(&'a str),
    DroppedHandle(&'a str),
    BlockingCall(&'a str, &'a str),
    BlockOn,
    // AsyncDeadlockCustom(&'a str),
    BarrierTimeout(f64),
}

impl<'a> RuntimeWarning<'a> {
    pub(crate) fn enable_warnings() {
        ENABLED.store(true, Ordering::Relaxed);
    }
    pub(crate) fn disable_warnings() {
        ENABLED.store(false, Ordering::Relaxed);
    }
    fn print_warning(&self) -> bool {
        if ENABLED.load(Ordering::Relaxed) {
            match self {
                RuntimeWarning::UnspawnedTask(_) => match config().unpspawned_task_warning {
                    Some(true) => true,
                    Some(false) => false,
                    None => true,
                },
                RuntimeWarning::DroppedHandle(_) => match config().dropped_unused_handle_warning {
                    Some(true) => true,
                    Some(false) => false,
                    None => true,
                },
                RuntimeWarning::BlockingCall(_, _) | RuntimeWarning::BlockOn => {
                    if std::thread::current().id() != *crate::MAIN_THREAD {
                        match config().blocking_call_warning {
                            Some(true) => true,
                            Some(false) => false,
                            None => true,
                        }
                    } else {
                        false
                    }
                }
                RuntimeWarning::BarrierTimeout(elapsed) => elapsed > &config().deadlock_timeout,
            }
        } else {
            false
        }
    }

    #[cfg(feature = "runtime-warnings-panic")]
    fn panic(&self, msg: &str) {
        match self {
            RuntimeWarning::BarrierTimeout(_) => {}
            _ => panic!("{msg}
                Note this warning causes a panic because you have comilpiled lamellar with the `runtime-warnings-panic` feature.
                Recompile without this feauture to only print warnings, rather than panic.
                To disable runtime warnings completely, recompile lamellar with the `disable-runtime-warnings` feature.
                To view backtrace set RUST_LIB_BACKTRACE=1.
                {}",
                std::backtrace::Backtrace::capture()),
        }
    }

    pub(crate) fn print(self) {
        #[cfg(not(feature = "disable-runtime-warnings"))]
        if self.print_warning() {
            let msg = match self {
                RuntimeWarning::UnspawnedTask(msg) => {
                    format!("[LAMELLAR WARNING] you have called {msg}. 
                    This typically means you forgot to call spawn() on the handle returned from calls such as exec_am_* or various array operations.
                    If this is your intended behavior, set LAMELLAR_UNSPAWNED_TASK_WARNING=0 to disable this warning.")
                }
                RuntimeWarning::DroppedHandle(msg) => {
                    format!("[LAMELLAR WARNING] You are dropping {msg} that has not been 'await'ed, 'spawn()'ed or 'block()'ed on a PE. 
                    This means any work associated with the AM will not be performed. Set LAMELLAR_DROPPED_UNUSED_HANDLE_WARNING=0 to disable this warning.")
                }
                RuntimeWarning::BlockingCall(func, async_func) => {
                    format!("[LAMELLAR WARNING] You are calling {func} from within an async context, this may result in deadlock! 
                    Using '{async_func}' is likely a better choice. Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning.")
                }
                RuntimeWarning::BlockOn => {
                    format!("[LAMELLAR WARNING] You are calling block_on from within an async context, this may result in deadlock! 
                    If you have something like: `world.block_on(my_future)` you can simply change to my_future.await.
                    If this is not the case, please file an issue on github. Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning.")
                }
                RuntimeWarning::BarrierTimeout(_) => {
                    format!("[LAMELLAR WARNING][{:?}] You have encoutered a barrier timeout. Potential deadlock detected.
                    Barrier is a collective operation requiring all PEs associated with the distributed object to enter the barrier call. 
                    Please refer to https://docs.rs/lamellar/latest/lamellar/index.html?search=barrier for more information.
                    Note that barriers are often called internally for many collective operations, including constructing new LamellarTeams, LamellarArrays, and Darcs, as well as distributed iteration. 
                    You may be seeing this message if you have called barrier within an async context (meaning it was executed on a worker thread).
                    A full list of collective operations is found at https://docs.rs/lamellar/latest/lamellar/index.html?search=collective .
                    The deadlock timeout can be set via the LAMELLAR_DEADLOCK_WARNING_TIMEOUT environment variable, the current timeout is {} seconds, setting this to 0 will disable this warning.",
                        std::thread::current().id(), config().deadlock_timeout)
                }
            };

            #[cfg(feature = "runtime-warnings-panic")]
            self.panic(&msg);
            println!(
                "{msg}
                Note that this warning is informative only, and will not terminate your application.
                To disable runtime warnings completely, recompile lamellar with the `disable-runtime-warnings` feature.
                Alternatively, you can force runtime warnings to cause a panic by recompiling with the  `runtime-warnings-panic` feature.
                To view backtrace set RUST_LIB_BACKTRACE=1.
                {}",
                std::backtrace::Backtrace::capture()
            );
        }
    }
}
