/// Lamellar Active Message for GPU kernel dispatch.
///
/// The payload carries only serializable values: array_id, kernel_name,
/// and LaunchConfig.  The `exec` body looks up the local array and kernel
/// from the process-local registries.
///
/// # Note on early returns
/// The `#[lamellar::am]` macro wraps the final expression of `exec` into
/// a `LamellarReturn`.  Using `return` statements inside `exec` bypasses
/// that wrapping and causes a type mismatch.  All control flow must
/// converge to a single value at the end — we use an inner async block
/// that returns `Result<(), GpuError>` and match on it at the end.
use crate::gpu::{array::local::get_array_entry, kernel::LaunchConfig};

/// Result returned by each PE after kernel execution.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GpuKernelResult {
    pub pe: usize,
    /// None = success; Some(msg) = error.
    pub error: Option<String>,
}

impl GpuKernelResult {
    pub fn ok(pe: usize) -> Self { Self { pe, error: None } }
    pub fn err(pe: usize, msg: impl Into<String>) -> Self { Self { pe, error: Some(msg.into()) } }
    pub fn is_ok(&self) -> bool { self.error.is_none() }
}

/// Active Message: run a registered kernel on this PE's local GPU array.
#[lamellar_impl::AmDataRT(Clone, Debug)]
pub struct ExecKernelAm {
    pub array_id: u64,
    pub kernel_name: String,
    pub config: LaunchConfig,
}

#[lamellar_impl::rt_am]
impl LamellarAM for ExecKernelAm {
    async fn exec(self) -> GpuKernelResult {
        let pe = lamellar::current_pe;

        // Use an inner block returning Result so we never use `return` here —
        // the `#[lamellar::am]` macro wraps the final expression and early
        // `return` bypasses that wrapping, causing a type mismatch.
        // NOTE: We only *launch* the kernel here — no stream sync.
        // Calling cudaStreamSynchronize inside an async AM task would block
        // the Lamellar executor thread and deadlock the .block() caller.
        // Callers must synchronize (e.g. cudaDeviceSynchronize / sync_local)
        // after exec_kernel_all().block() returns.
        let result: Result<(), String> = async {
            let entry = get_array_entry(self.array_id)
                .map_err(|e| format!("{e}"))?;

            let mut guard = entry.write();
            guard.launch_kernel(&self.kernel_name, &self.config)
                .map_err(|e| format!("{e}"))?;

            Ok(())
        }
        .await;

        match result {
            Ok(()) => GpuKernelResult::ok(pe),
            Err(msg) => GpuKernelResult::err(pe, msg),
        }
    }
}
