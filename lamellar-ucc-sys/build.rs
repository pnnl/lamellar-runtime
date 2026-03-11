
use std::{env, path::PathBuf};
use std::path::Path;

fn copy_rec(src: &Path, dst: &Path) -> std::io::Result<()> {
    if src.is_dir() {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            copy_rec(&src_path, &dst_path)?;
        }
    } else {
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(src, dst)?;
    }
    Ok(())
}

fn build_ucc(out_path: &PathBuf) -> PathBuf {
    let ucx_root_path = std::env::var("DEP_UCX_ROOT").expect("Could not find UCX installation.");
    let _ucx_lib_path = std::path::PathBuf::from(&ucx_root_path).join("lib");
    let _ucx_include_path = std::path::PathBuf::from(&ucx_root_path).join("include");

    
    let dest = out_path.clone().join("ucc_src");
    copy_rec(&std::path::PathBuf::from("ucc"), &dest).expect("Failed to copy UCC source files");
    
    let src_path = std::fs::canonicalize(&dest).unwrap();
    std::process::Command::new("./autogen.sh")
            .current_dir(src_path.as_path())
            .status()
            .expect("Failed to autogen for ucc");

    let path = autotools::Config::new(src_path)
        .reconf("-ivf")
        .enable_shared()
        .disable_static()
        .with("ucx", Some(&ucx_root_path))
        .build();
    path
}

fn build_bindings() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-env-changed=UCC_DIR");
    let ucc_path = match env::var("UCC_DIR") {
        Ok(val) => std::path::PathBuf::from(val),
        Err(_) => {
            let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
             println!("cargo:warning=UCC_DIR environment variable not set, building UCC from source. This may take a while. To use a pre-built version of UCC, set the UCC_DIR environment variable to the path of the UCC installation.");
             build_ucc(&out_dir)
        }
    };
    println!("cargo:root={}", ucc_path.display());

    let mut builder = cc::Build::new();
    builder.file("wrapper.c");
    builder.include(&ucc_path.join("./include"));
    builder.compile("wrapper");

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg(format!("-I{}", ucc_path.join("include").display()))
        .generate()
        .expect("Unable to generate bindings for UCC");

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

    let ucc_lib_path = ucc_path.join("lib");
    println!(
        "cargo:rustc-link-arg=-Wl,-rpath,{}",
        ucc_lib_path.display()
    );
    println!("cargo:rustc-link-search={}", ucc_lib_path.display());
    if let Ok(ucx_root_path) = env::var("DEP_UCX_ROOT") {
        let ucx_lib_path = std::path::PathBuf::from(ucx_root_path).join("lib");
        println!(
            "cargo:rustc-link-arg=-Wl,-rpath,{}",
            ucx_lib_path.display()
        );
        println!("cargo:rustc-link-search={}", ucx_lib_path.display());
    }
    println!("cargo:rustc-link-lib=ucc");
    println!("cargo:rustc-link-lib=ucs");
    println!("cargo:rustc-link-lib=ucm");
}


fn main() {
    println!("cargo:rerun-if-changed={}", "build.rs");
    build_bindings();
}