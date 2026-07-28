use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

fn main() {
    let mut lib_paths: Vec<String> = Vec::new();

    println!("cargo:rerun-if-env-changed=DEP_OFI_ROOT");
    #[cfg(any(feature = "libfabric", feature = "libfabric-sys"))]
    {
        if let Ok(fabric_lib_dir) = env::var("DEP_OFI_ROOT") {
            let lib_path = PathBuf::from(fabric_lib_dir).join("lib");
            println!("cargo:rustc-link-search=native={}", lib_path.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
            lib_paths.push(lib_path.display().to_string());
        } else {
            println!("cargo:warning=DEP_OFI_ROOT not set; skipping libfabric lib path");
        }
    }

    println!("cargo:rerun-if-env-changed=DEP_ROFI_ROOT");
    #[cfg(feature = "enable-rofi-c-shared")]
    {
        if let Ok(rofi_lib_dir) = env::var("DEP_ROFI_ROOT") {
            let lib_path = PathBuf::from(rofi_lib_dir).join("lib");
            println!("cargo:rustc-link-search=native={}", lib_path.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
            lib_paths.push(lib_path.display().to_string());
        } else {
            panic!(
                "unable to set rofi backend, recompile with 'enable-rofi' feature {:?}",
                env::vars()
            )
        }
    }
    println!("cargo:rerun-if-env-changed=DEP_UCX_ROOT");
    println!("cargo:rerun-if-env-changed=DEP_UCC_ROOT");
    #[cfg(feature = "enable-ucx")]
    {
        if let Ok(lamellar_ucx_lib_dir) = env::var("DEP_UCX_ROOT") {
            let lib_path = PathBuf::from(lamellar_ucx_lib_dir).join("lib");
            println!("cargo:rustc-link-search=native={}", lib_path.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
            lib_paths.push(lib_path.display().to_string());
        } else {
            panic!(
                "unable to set lamellar-ucx backend, recompile with 'enable-ucx' feature {:?}",
                env::vars()
            )
        }

        if let Ok(lamellar_ucc_root) = env::var("DEP_UCC_ROOT") {
            let lib_path = PathBuf::from(lamellar_ucc_root).join("lib");
            println!("cargo:rustc-link-search=native={}", lib_path.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
            lib_paths.push(lib_path.display().to_string());
        }
    }

    println!("cargo:rerun-if-env-changed=DEP_PMINATIVE_ROOT");
    if let Ok(pmi_native_lib_dir) = env::var("DEP_PMINATIVE_ROOT") {
        let lib_path = PathBuf::from(pmi_native_lib_dir).join("lib");
        // println!(
        //     "cargo:warning=Adding PMI native lib path: {}",
        //     lib_path.display()
        // );
        println!("cargo:rustc-link-search=native={}", lib_path.display());
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
        lib_paths.push(lib_path.display().to_string());
    } else {
        println!("cargo:warning=DEP_PMINATIVE_ROOT not set; skipping PMI native lib path");
    }
    // println!("cargo:warning={:?}", env::vars());

    let out_dir = env::var("OUT_DIR").unwrap_or_else(|_| ".".to_string());
    // println!("cargo:warning=outdir: {out_dir:?}");
    let out_path = PathBuf::from(&out_dir);
    let profile_output_dir = determine_profile_output_dir(&out_path);
    copy_dependency_libs(&lib_paths, &profile_output_dir);

    // if let Ok(origin) = env::var("ORIGIN") {
    //     println!("cargo:warning=rpath for sharedlibs: {}/shared_libs", origin);
    // }
    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/shared_libs");

    // Link system libraries required by libfabric/ROFI providers
    // Some providers reference libnuma and libuuid symbols (e.g. uuid_unparse, numa_*).
    // Ensure the linker includes those libraries when building with these features.
    #[cfg(any(
        feature = "enable-rofi-c",
        feature = "enable-rofi-c-shared",
        feature = "enable-libfabric"
    ))]
    {
        println!("cargo:rustc-link-lib=dylib=numa");
        println!("cargo:rustc-link-lib=dylib=uuid");
    }

    // Generate bash script with library paths
    // Navigate from OUT_DIR to the workspace root (where cargo was invoked)

    // OUT_DIR is typically target/debug/build/<crate>/out
    // Navigate up to find the project root (where target/ is)
    let mut script_dir = out_path.clone();
    while script_dir.file_name().is_some() {
        if script_dir.join("Cargo.toml").exists() || script_dir.join("target").exists() {
            break;
        }
        if !script_dir.pop() {
            // Fallback to current directory if we can't find project root
            script_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            break;
        }
    }

    let script_path = script_dir.join("lamellar_env.sh");

    if let Ok(mut file) = File::create(&script_path) {
        writeln!(file, "#!/bin/bash").ok();
        writeln!(file, "# Auto-generated by build.rs - DO NOT EDIT").ok();
        writeln!(
            file,
            "# Source this file before running lamellar applications"
        )
        .ok();
        writeln!(file, "# Usage: source lamellar_env.sh").ok();
        writeln!(file, "").ok();

        if !lib_paths.is_empty() {
            write!(file, "export LD_LIBRARY_PATH=").ok();
            for (i, path) in lib_paths.iter().enumerate() {
                if i > 0 {
                    write!(file, ":").ok();
                }
                write!(file, "{}", path).ok();
            }
            writeln!(file, ":$LD_LIBRARY_PATH").ok();
        }

        // println!(
        //     "cargo:warning=Generated library path script: {}",
        //     script_path.display()
        // );
    } else {
        panic!("Failed to create lamellar_env.sh");
    }
}

fn determine_profile_output_dir(out_dir: &Path) -> PathBuf {
    let mut cursor = out_dir;
    loop {
        if cursor.file_name().and_then(|name| name.to_str()) == Some("build") {
            return cursor
                .parent()
                .expect("build directory should have a parent")
                .to_path_buf();
        }

        cursor = cursor
            .parent()
            .expect("OUT_DIR should contain a build directory");
    }
}

fn copy_dependency_libs(lib_paths: &[String], output_dir: &Path) {
    if lib_paths.is_empty() {
        return;
    }

    let shared_libs_dir = output_dir.join("deps");
    if let Err(err) = fs::create_dir_all(&shared_libs_dir) {
        println!(
            "cargo:warning=Unable to create shared libs directory {}: {}",
            shared_libs_dir.display(),
            err
        );
        return;
    }

    println!(
        "cargo:rustc-env=LAMELLAR_SHARED_LIBS_DIR={}",
        shared_libs_dir.display()
    );
    println!(
        "cargo:rustc-link-arg=-Wl,-rpath,{}",
        shared_libs_dir.display()
    );

    std::env::set_var("LAMELLAR_SHARED_LIBS_DIR", &shared_libs_dir);
    // std::env::set_var("LD_LIBRARY_PATH", format!("{}:{}", shared_libs_dir.display(), std::env::var("LD_LIBRARY_PATH").unwrap_or_default()));

    let mut candidates = HashMap::<String, Candidate>::new();
    for lib_path in lib_paths {
        let lib_dir = PathBuf::from(lib_path);
        if !lib_dir.is_dir() {
            continue;
        }

        let entries = match fs::read_dir(&lib_dir) {
            Ok(entries) => entries,
            Err(err) => {
                println!(
                    "cargo:warning=Unable to read library directory {}: {}",
                    lib_dir.display(),
                    err
                );
                continue;
            }
        };

        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let file_name = entry.file_name();
            let file_name_str = match file_name.to_str() {
                Some(s) => s,
                None => continue,
            };
            if !is_shared_object(file_name_str) {
                continue;
            }

            let (base, is_exact) = match shared_object_base(file_name_str) {
                Some(result) => result,
                None => continue,
            };

            let candidate = Candidate {
                path: path.clone(),
                name: file_name.clone(),
                is_exact,
            };

            match candidates.entry(base) {
                std::collections::hash_map::Entry::Occupied(mut existing) => {
                    if !existing.get().is_exact && candidate.is_exact {
                        existing.insert(candidate);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(candidate);
                }
            }
        }
    }

    let mut linked = Vec::new();
    for candidate in candidates.values() {
        let dest = shared_libs_dir.join(&candidate.name);
        if dest.exists() {
            let _ = fs::remove_file(&dest);
        }
        if let Err(err) = symlink(&candidate.path, &dest) {
            println!(
                "cargo:warning=Failed to link {} into {}: {}",
                candidate.path.display(),
                dest.display(),
                err
            );
            continue;
        }
        linked.push(candidate.name.clone());
    }

    // if !linked.is_empty() {
    //     println!(
    //         "cargo:warning=Linked {} shared libs into {}",
    //         linked.len(),
    //         shared_libs_dir.display()
    //     );
    // }
}

fn is_shared_object(file_name: &str) -> bool {
    file_name.contains(".so")
}

fn shared_object_base(file_name: &str) -> Option<(String, bool)> {
    let pos = file_name.find(".so")?;
    let is_exact = file_name[pos + 3..].is_empty();
    Some((file_name[..pos].to_string(), is_exact))
}

#[derive(Clone)]
struct Candidate {
    path: PathBuf,
    name: OsString,
    is_exact: bool,
}
