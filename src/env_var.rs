//! Lamellar uses a number of environment variables to configure its behavior
//! the following variables are supported along with a brief description and default value
//!
//! - `LAMELLAR_BACKEND` - the backend used during execution. Note that if a backend is explicitly set in the world builder, this variable is ignored.
//!     - possible values
//!         - `local` -- default (if none of `enable-rofi-c`, `enable-libfabric-sys`, `enable-libfabric`, `enable-libfabric-async`, or `enable-ucx` features are active)
//!         - `shmem`
//!         - `rofi_c`  -- only available with the `enable-rofi-c` feature; default if active, checked first
//!         - `libfabric-sys` -- only available with the `enable-libfabric-sys` feature; default if active and `enable-rofi-c` is not
//!         - `libfabric` -- only available with the `enable-libfabric` feature; default if active and neither `enable-rofi-c` nor `enable-libfabric-sys` are
//!         - `libfabric-async` -- only available with the `enable-libfabric-async` feature; default if active and none of `enable-rofi-c`, `enable-libfabric-sys`, `enable-libfabric` are
//!         - `ucx` -- only available with the `enable-ucx` feature; default if active and none of `enable-rofi-c`, `enable-libfabric-sys`, `enable-libfabric`, `enable-libfabric-async` are
//! - `LAMELLAR_EXECUTOR` - the executor used during execution. Note that if a executor is explicitly set in the world builder, this variable is ignored.
//!     - possible values
//!         - `lamellar` -- default, work stealing backend
//!         - `async_std` -- alternative backend from async_std
//!         - `tokio` -- only available with the `tokio-executor` feature in which case it is the default executor
//! - `LAMELLAR_BATCHER` - selects how small active messages are batched for remote operations
//!     - possible values
//!         - `simple` -- default, active messages are only batched based on the PE they are sent to
//!         - `direct` -- stages batched messages into a local Vec before copying into transport buffers at flush time
//!         - `team_am` -- active messages are batched hierarchically based on the remote PE, team sending the message, and AM id
//! - `LAMELLAR_THREADS` - The number of worker threads used within a lamellar PE, defaults to [std::thread::available_parallelism] if available or else 4
//! - `LAMELLAR_HEAP_SIZE` - Specify the initial size of the Runtime "RDMAable" memory pool. Defaults to 4GB
//!     - Internally, Lamellar utilizes memory pools of RDMAable memory for Runtime data structures (e.g. [Darcs][crate::Darc],
//!       [OneSidedMemoryRegion][crate::memregion::OneSidedMemoryRegion],etc), aggregation buffers, and message queues.
//!     - Note: when running multiple PEs on a single system, the total allocated memory for the pools would be equal to `LAMELLAR_HEAP_SIZE * number of processes`
//! - `LAMELLAR_HEAP_MODE` - Specify whether the heap will be allocated statically or dynamically
//!     - possible values
//!         - `static`
//!         - `dynamic` -- default, Additional memory pools are dynamically allocated across the system as needed.
//!           This can be a fairly expensive operation (as the operation is synchronous across all PEs) so the runtime
//!           will print a message at the end of execution with how many additional pools were allocated.
//!              - if you find you are dynamically allocating new memory pools, try setting `LAMELLAR_HEAP_SIZE` to a larger value
//! - `LAMELLAR_DEADLOCK_WARNING_TIMEOUT` - the timeout in seconds before a deadlock warning is printed. Defaults to 600, set to 0 to disable. Note this does not cause your application to terminate
//! - `LAMELLAR_AM_GROUP_BATCH_SIZE` - The maximum number of sub messages that will be sent in a single AMGroup Active Message, default: 10000
//! - `LAMELLAR_BLOCKING_CALL_WARNING` - flag used to print warnings when users call barriers on worker threads. Default: true
//! - `LAMELLAR_DROPPED_UNUSED_HANDLE_WARNING` - flag used to print warnings when users drop active message handles without awaiting, spawning, or blocking on them. Default:
//! - `LAMELLAR_UNSPAWNED_TASK_WARNING` - flag used to print warnings when users attempt to call wait_all while there are tasks that have not been spawned. Default: true
//! - `LAMELLAR_BARRIER_DISSEMINATION_FACTOR` - (Experimental) The dissemination factor for the n-way barrier, default: 2
//! - `LAMELLAR_BATCH_OP_THREADS` - the number of threads used to initiate batched operations, defaults to 1/4 LAMELLAR_THREADS
//! - `LAMELLAR_ARRAY_INDEX_SIZE` - specify static or dynamic array index size
//!     - possible values
//!         - `static` -- constant usize indices
//!         - `dynamic` -- default, only uses as large an int as necessary to index the array, bounded by the max number of elements on any PE.
//! - `LAMELLAR_AM_SIZE_THRESHOLD` - the threshold for an activemessage (in bytes) on whether it will be sent directly or aggregated, default: 100000
//! - `LAMELLAR_SCALAR_INLINE_THRESHOLD` - the threshold (in bytes) below which a scalar (POD) array batch-op payload is inlined
//!   directly into its active message instead of being allocated from the RDMAable memory pool and fetched via a GET, default: 524288
//! - `LAMELLAR_ROFI_PROVIDER` - the provider for the rofi backend (only used with the rofi backend), default: "verbs"
//! - `LAMELLAR_ROFI_DOMAIN` - the domain for the rofi backend (only used with the rofi backend), default: ""
//! - `LAMELLAR_DISABLE_ON_NODE_SHMEM` - set to true or 1 to disable same-node shared-memory fast path (UCX/libfabric), default: false
//! - `LAMELLAR_CMD_QUEUE` - selects the command queue protocol variant
//!     - possible values
//!         - `batched` -- GET-based protocol that batches multiple outgoing commands into shared buffers before flushing
//!         - `get` -- default, receiver issues RDMA GET for data
//!         - `geteager` -- GET-based protocol with eager send for small messages
//!         - `getslots` -- GET-based protocol with multiple in-flight slots per PE pair
//!         - `put` -- receiver allocates a buffer, sender PUTs data and completion signal directly
//!         - `putslots` -- PUT-based protocol with multiple in-flight slots per PE pair
//!         - `puteager` -- PUT-based protocol with eager send for small messages
use serde::Deserialize;
use std::sync::OnceLock;

fn default_deadlock_warning_timeout() -> f64 {
    600.0
}

fn default_am_group_batch_size() -> usize {
    10000
}

fn default_dissemination_factor() -> usize {
    2
}

fn default_backend() -> String {
    if cfg!(feature = "enable-rofi-c") {
        return "rofi_c".to_owned();
    } else if cfg!(feature = "enable-libfabric-sys") {
        return "libfabric-sys".to_owned();
    } else if cfg!(feature = "enable-libfabric") {
        return "libfabric".to_owned();
    } else if cfg!(feature = "enable-libfabric-async") {
        return "libfabric-async".to_owned();
    } else if cfg!(feature = "enable-ucx") {
        return "ucx".to_owned();
    } else {
        return "local".to_owned();
    }
}

/// The lamellae backend that will be used if `LAMELLAR_BACKEND` is unset, based on which
/// backend features this build was compiled with.
pub fn compiled_default_backend() -> String {
    default_backend()
}

/// The lamellae backends available in this build, based on which backend features were
/// enabled at compile time. `local` and `shmem` are always available.
pub fn available_backends() -> Vec<&'static str> {
    let mut backends = Vec::new();
    if cfg!(feature = "enable-rofi-c") {
        backends.push("rofi_c");
    }
    if cfg!(feature = "enable-libfabric-sys") {
        backends.push("libfabric-sys");
    }
    if cfg!(feature = "enable-libfabric") {
        backends.push("libfabric");
    }
    if cfg!(feature = "enable-libfabric-async") {
        backends.push("libfabric-async");
    }
    if cfg!(feature = "enable-ucx") {
        backends.push("ucx");
    }
    backends.push("shmem");
    backends.push("local");
    backends
}

fn default_executor() -> String {
    #[cfg(feature = "tokio-executor")]
    return "tokio".to_owned();
    #[cfg(not(feature = "tokio-executor"))]
    return "lamellar".to_owned();
}

fn default_batcher() -> String {
    "simple".to_owned()
}

fn default_threads() -> usize {
    #[cfg(doctest)]
    return 1;
    match std::thread::available_parallelism() {
        Ok(n) => n.into(),
        Err(_) => 4,
    }
}

#[doc(hidden)]
#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum HeapMode {
    Static,
    Dynamic,
}

fn default_heap_mode() -> HeapMode {
    HeapMode::Dynamic
}

#[doc(hidden)]
#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Alloc {
    Heap,
    Lamellae,
}

fn default_alloc() -> Alloc {
    Alloc::Heap
}

#[doc(hidden)]
#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum IndexType {
    Static,
    Dynamic,
}
fn default_array_dynamic_index() -> IndexType {
    IndexType::Dynamic
}

#[doc(hidden)]
#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CmdQueue {
    /// GET-based protocol that batches multiple outgoing commands into shared buffers before flushing
    Batched,
    /// GET-based protocol: receiver GETs data from sender (default)
    Get,
    /// GET-based protocol with eager send for small messages (≤4096 bytes)
    GetEager,
    /// GET-based protocol with N=4 in-flight slots per PE pair
    GetSlots,
    /// PUT-based protocol: receiver allocates buffer, sender PUTs data and completion signal directly
    Put,
    /// PUT-based protocol with N=4 in-flight slots per PE pair
    PutSlots,
    /// PUT-based protocol with eager send for small messages (≤4096 bytes)
    PutEager,
}

fn default_cmd_queue() -> CmdQueue {
    CmdQueue::Get
}

fn default_cmd_buf_len() -> usize {
    50000
}

fn default_cmd_buf_cnt() -> usize {
    2
}

fn default_am_size_threshold() -> usize {
    100000
}

fn default_scalar_inline_threshold() -> usize {
    524288
}

fn default_rofi_provider() -> String {
    "verbs".to_owned()
}

fn default_rofi_domain() -> String {
    "".to_owned()
}

fn deserialize_bool_or_int_to_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if let Ok(int) = s.parse::<u8>() {
        Ok(Some(int != 0))
    } else if let Ok(bool) = s.parse::<bool>() {
        Ok(Some(bool))
    } else {
        Err(serde::de::Error::custom(format!(
            "invalid boolean value: {}",
            s
        )))
    }
}

fn default_ucc_oob_init_buffer_size() -> usize {
    16 * 1024
}

#[doc(hidden)]
#[derive(Deserialize, Debug)]
pub struct Config {
    /// A general timeout in seconds for various operations which may indicate a deadlock, default: 600.0 seconds
    #[serde(default = "default_deadlock_warning_timeout")]
    pub deadlock_warning_timeout: f64,

    /// The maximum number of sub messages that will be sent in a single AMGroup Active Message, default: 10000
    #[serde(default = "default_am_group_batch_size")]
    pub am_group_batch_size: usize, // am group batch size

    /// The dissemination factor for the n-way barrier, default: 2
    #[serde(default = "default_dissemination_factor")]
    pub barrier_dissemination_factor: usize,

    /// flag used to print warnings when users call barriers on worker threads. Default: true
    #[serde(deserialize_with = "deserialize_bool_or_int_to_bool", default)]
    pub blocking_call_warning: Option<bool>,

    /// flag used to print warnings when users drop active message handles without awaiting, spawning, or blocking on them. Default: true
    #[serde(deserialize_with = "deserialize_bool_or_int_to_bool", default)]
    pub dropped_unused_handle_warning: Option<bool>,

    /// flag used to print warnings when users attempt to call wait_all while there are tasks that have not been spawned. Default: true
    #[serde(deserialize_with = "deserialize_bool_or_int_to_bool", default)]
    pub unspawned_task_warning: Option<bool>,

    /// The lamellae backend to use
    /// rofi -- multi pe distributed execution, default if rofi feature is turned on
    /// local -- single pe execution, default if rofi feature is turned off
    /// shmem -- multi pe single node execution
    #[serde(default = "default_backend")]
    pub backend: String, //rofi,shmem,local

    /// The executor (thread scheduler) to use, default: 'lamellar' unless the tokio feature is turned on
    #[serde(default = "default_executor")]
    pub executor: String, //lamellar,tokio,async_std

    /// The batcher to use, default: 'simple'
    #[serde(default = "default_batcher")]
    pub batcher: String,
    #[serde(default = "default_threads")]
    pub threads: usize,
    pub batch_op_threads: Option<usize>, //number of threads used to process array batch ops sending
    pub heap_size: Option<usize>,
    #[serde(default = "default_heap_mode")]
    pub heap_mode: HeapMode,
    #[serde(default = "default_alloc")]
    pub alloc: Alloc,
    #[serde(default = "default_array_dynamic_index")]
    pub array_index_size: IndexType,

    //used internally by the command queues
    #[serde(default = "default_cmd_buf_len")]
    pub cmd_buf_len: usize,
    //used internally by the command queues
    #[serde(default = "default_cmd_buf_cnt")]
    pub cmd_buf_cnt: usize,
    /// Command queue protocol variant: `batched`, `get` (default), `geteager`, `getslots`, `put`, `putslots`, or `puteager`
    #[serde(default = "default_cmd_queue")]
    pub cmd_queue: CmdQueue,

    #[serde(default = "default_am_size_threshold")]
    pub am_size_threshold: usize, //the threshold for an activemessage (in bytes) on whether it will be sent directly or aggregated
    /// Threshold (bytes) below which a scalar array batch-op payload is inlined into its AM instead of using the RDMAable memory pool + GET, default: 524288
    #[serde(default = "default_scalar_inline_threshold")]
    pub scalar_inline_threshold: usize,
    #[serde(default = "default_rofi_provider")]
    pub rofi_provider: String,
    #[serde(default = "default_rofi_domain")]
    pub rofi_domain: String,

    /// Disable same-node shared-memory fast path for UCX/libfabric backends, default: false
    #[serde(deserialize_with = "deserialize_bool_or_int_to_bool", default)]
    pub disable_on_node_shmem: Option<bool>,
    #[serde(default = "default_ucc_oob_init_buffer_size")]
    pub ucc_oob_init_buffer_size: usize,
}

#[doc(hidden)]
/// Get the current Environment Variable configuration
pub fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(|| match envy::prefixed("LAMELLAR_").from_env::<Config>() {
        Ok(config) => config,
        Err(error) => panic!("{}", error),
    })
}
