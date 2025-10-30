#[cfg(any(feature = "enable-rofi-c-shared", feature = "enable-ucx"))]
use std::env;
#[cfg(any(feature = "enable-rofi-c-shared", feature = "enable-ucx"))]
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=DEP_ROFI_ROOT");
    #[cfg(feature = "enable-rofi-c-shared")]
    {
        if let Ok(rofi_lib_dir) = env::var("DEP_ROFI_ROOT") {
            let lib_path = PathBuf::from(rofi_lib_dir).join("lib");
            println!("cargo:rustc-link-search=native={}", lib_path.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
        } else {
            panic!(
                "unable to set rofi backend, recompile with 'enable-rofi' feature {:?}",
                env::vars()
            )
        }
    }
    println!("cargo:rerun-if-env-changed=DEP_UCX_ROOT");
    #[cfg(feature = "enable-ucx")]
    {
        if let Ok(lamellar_ucx_lib_dir) = env::var("DEP_UCX_ROOT") {
            let lib_path = PathBuf::from(lamellar_ucx_lib_dir).join("lib");
            println!("cargo:rustc-link-search=native={}", lib_path.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
        } else {
            panic!(
                "unable to set lamellar-ucx backend, recompile with 'enable-ucx' feature {:?}",
                env::vars()
            )
        }
    }
}
