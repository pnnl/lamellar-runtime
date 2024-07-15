use std::sync::OnceLock;

use serde::Deserialize;

fn default_deadlock_timeout() -> f64 {
    600.0
}

fn default_am_group_batch_size() -> usize {
    10000
}

fn default_dissemination_factor() -> usize {
    2
}

fn default_backend() -> String {
    #[cfg(feature = "enable-rofi")]
    return "rofi".to_owned();
    #[cfg(not(feature = "enable-rofi"))]
    return "local".to_owned();
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
    match std::thread::available_parallelism() {
        Ok(n) => n.into(),
        Err(_) => 4,
    }
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum HeapMode {
    Static,
    Dynamic,
}

fn default_heap_mode() -> HeapMode {
    HeapMode::Dynamic
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Alloc {
    Heap,
    Lamellae,
}

fn default_alloc() -> Alloc {
    Alloc::Heap
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum IndexType {
    Static,
    Dynamic,
}
fn default_array_dynamic_index() -> IndexType {
    IndexType::Dynamic
}

fn default_cmd_buf_len() -> usize {
    50000
}

fn default_cmd_buf_cnt() -> usize {
    2
}

fn default_batch_am_size() -> usize {
    100000
}

#[derive(Deserialize, Debug)]
pub struct Config {
    /// A general timeout in seconds for various operations which may indicate a deadlock, default: 600.0 seconds
    #[serde(default = "default_deadlock_timeout")]
    pub deadlock_timeout: f64,

    /// The maximum number of sub messages that will be sent in a single AMGroup Active Message, default: 10000
    #[serde(default = "default_am_group_batch_size")]
    pub am_group_batch_size: usize, // am group batch size

    /// The dissemination factor for the n-way barrier, default: 2
    #[serde(default = "default_dissemination_factor")]
    pub barrier_dissemination_factor: usize,

    /// flag used to print warnings when users call barriers on worker threads. Default: true
    pub barrier_warning: Option<bool>,

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
    pub index_size: IndexType,
    #[serde(default = "default_cmd_buf_len")]
    pub cmd_buf_len: usize,
    #[serde(default = "default_cmd_buf_cnt")]
    pub cmd_buf_cnt: usize,
    #[serde(default = "default_batch_am_size")]
    pub batch_am_size: usize, //the threshold for an activemessage (in bytes) on whether it will be sent directly or aggregated
}

/// Get the current Environment Variable configuration
pub fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(|| match envy::prefixed("LAMELLAR_").from_env::<Config>() {
        Ok(config) => config,
        Err(error) => panic!("{}", error),
    })
}
