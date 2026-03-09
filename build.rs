use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=DEP_ROFI_ROOT");
    #[cfg(feature = "enable-rofi-shared")]
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

    build_cuda_kernels();
}

/// Compile CUDA C kernels when the `cuda` feature is active.
///
/// Invokes nvcc directly to avoid linker-path issues in Cargo's build-script
/// environment.  nvcc's `-lib` flag produces a static archive that Cargo picks
/// up automatically.
///
/// Environment overrides
/// ─────────────────────
/// CUDA_ARCH — SM target, e.g. "sm_89" (default: sm_89 / RTX 4090 Ada Lovelace)
/// CUDA_BIN  — directory containing nvcc (default: /usr/local/cuda/bin)
/// CUDA_LIB  — directory containing libcudart (default: /usr/local/cuda/lib64)
/// CXX       — host C++ compiler passed to nvcc -ccbin (default: /usr/bin/g++)
fn build_cuda_kernels() {
    if env::var("CARGO_FEATURE_CUDA").is_err() {
        return;
    }

    println!("cargo:rerun-if-changed=cuda/histo.cu");
    println!("cargo:rerun-if-changed=cuda/topsort.cu");
    println!("cargo:rerun-if-env-changed=CUDA_ARCH");
    println!("cargo:rerun-if-env-changed=CUDA_BIN");
    println!("cargo:rerun-if-env-changed=CUDA_LIB");
    println!("cargo:rerun-if-env-changed=CXX");

    let arch = env::var("CUDA_ARCH").unwrap_or_else(|_| "sm_89".to_string());
    let cuda_bin = env::var("CUDA_BIN")
        .unwrap_or_else(|_| "/usr/local/cuda/bin".to_string());
    let cuda_lib = env::var("CUDA_LIB")
        .unwrap_or_else(|_| "/usr/local/cuda/lib64".to_string());
    let cxx = env::var("CXX")
        .or_else(|_| find_cxx())
        .unwrap_or_else(|_| "/usr/bin/g++".to_string());

    let nvcc = PathBuf::from(&cuda_bin).join("nvcc");
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let lib_path = out_dir.join("libgpu_kernels.a");

    // Augment PATH so g++ can find `as` (assembler) in Cargo's restricted env.
    let path = env::var("PATH").unwrap_or_default();
    let augmented_path = format!("{cuda_bin}:/usr/bin:/bin:{path}");

    let status = Command::new(&nvcc)
        .env("PATH", &augmented_path)
        .args([
            "-lib",
            &format!("-arch={arch}"),
            &format!("-ccbin={cxx}"),
            "-Xcompiler=-w",
            "-o", lib_path.to_str().unwrap(),
            "cuda/histo.cu",
            "cuda/topsort.cu",
        ])
        .status()
        .unwrap_or_else(|e| panic!("Failed to run nvcc ({nvcc:?}): {e}"));

    if !status.success() {
        panic!("nvcc compilation failed (exit {status})");
    }

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=gpu_kernels");
    println!("cargo:rustc-link-search=native={cuda_lib}");
    println!("cargo:rustc-link-lib=cudart");
    // CUDA objects use C++ static initializers; need libstdc++ for cxa_guard_* symbols.
    // clang doesn't search the gcc-specific directory by default, so add it explicitly.
    println!("cargo:rustc-link-search=native=/usr/lib/gcc/x86_64-linux-gnu/13");
    println!("cargo:rustc-link-lib=stdc++");
}

fn find_cxx() -> Result<String, ()> {
    for candidate in ["/usr/bin/g++", "/usr/bin/c++", "/usr/local/bin/g++"] {
        if std::path::Path::new(candidate).exists() {
            return Ok(candidate.to_string());
        }
    }
    Err(())
}
