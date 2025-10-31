use std::{
    env,
    path::{Path, PathBuf},
    process::{Command, Stdio, exit},
};

use glob::glob;

fn build_ucx(out_path: &PathBuf) -> PathBuf {
    if !Path::new("ucx/.git").exists() {
        let _ = Command::new("git")
            .args(&["submodule", "update", "--init"])
            .status();
        let _ = Command::new("git")
            .current_dir("ucx")
            .args(&["checkout", "v1.19.0"])
            .status();
    }

    let dest = out_path.clone().join("ucx_src");
    Command::new("cp")
        .args(&["-r", "ucx", &dest.to_string_lossy()])
        .status()
        .unwrap();

    let path = autotools::Config::new(dest)
        .reconf("-ivfWnone")
        .enable("shared", None)
        .disable("static", None)
        .disable("logging", None)
        .disable("debug", None)
        .disable("assertions", None)
        .disable("params-check", None)
        .enable("optimizations", None)
        .enable("mt", None)
        .without("rocm", None)
        .without("cuda", None)
        .without("go", None)
        .without("java", None)
        .with("march", None)
        .build();

    // out_path.clone()
    path
}

fn check_lib_for_function(lib: PathBuf, func: &str) -> Option<()> {
    let nm_out = Command::new("nm")
        .args(&["-g", &lib.to_string_lossy()])
        .stdout(Stdio::piped())
        .spawn()
        .ok()?;
    let grep1 = Command::new("grep")
        .arg(func)
        .stdin(Stdio::from(nm_out.stdout?)) // Pipe through.
        .stdout(Stdio::piped())
        .spawn()
        .ok()?;
    let grep2 = Command::new("grep")
        .arg("T")
        .stdin(Stdio::from(grep1.stdout?)) // Pipe through.
        .output()
        .ok()?;
    if grep2.stdout.len() > 0 {
        return Some(());
    } else {
        return None;
    }
}

fn build_bindings() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    let ucx_env = match env::var("UCX_DIR") {
        Ok(val) => {
            let ucx_path = std::path::PathBuf::from(val);
            let ucx_lib_dir = ucx_path.join("lib");
            let ucm_a = ucx_lib_dir.join("libucm.a");
            let ucm_so = ucx_lib_dir.join("libucm.so");
            let ucp_a = ucx_lib_dir.join("libucp.a");
            let ucp_so = ucx_lib_dir.join("libucp.so");
            let ucs_a = ucx_lib_dir.join("libucs.a");
            let ucs_so = ucx_lib_dir.join("libucs.so");
            let uct_a = ucx_lib_dir.join("libuct.a");
            let uct_so = ucx_lib_dir.join("libuct.so");
            if check_lib_for_function(ucm_a, "ucm_set_global_opts").is_some()
                && check_lib_for_function(ucp_a, "ucp_lib_query").is_some()
                && check_lib_for_function(ucs_a, "ucs_init_ucm_opts").is_some()
                && check_lib_for_function(uct_a, "uct_query_components").is_some()
            {
                ucx_path
            } else if check_lib_for_function(ucm_so, "ucm_set_global_opts").is_some()
                && check_lib_for_function(ucp_so, "ucp_lib_query").is_some()
                && check_lib_for_function(ucs_so, "ucs_init_ucm_opts").is_some()
                && check_lib_for_function(uct_so, "uct_query_components").is_some()
            {
                ucx_path
            } else {
                println!(
                    "cargo:warning=unable to detect ucx version at {:?}. SUGGESTED: Rofisys includes a bundled version of Rofi it can build itself, simply unset the ROFI_DIR env variable to use the bundled version. ALTERNATIVE: update to version 0.3 of Rofi manually.",
                    ucx_path
                );
                exit(1);
            }
        }
        Err(_) => build_ucx(&out_path),
    };
    for entry in glob("ucx/**/*.c").expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => println!("cargo:rerun-if-changed={}", path.display()),
            Err(_) => {}
        }
    }
    for entry in glob("ucx/**/*.h").expect("Failed to read glob pattern") {
        match entry {
            Ok(path) => println!("cargo:rerun-if-changed={}", path.display()),
            Err(_) => {}
        }
    }

    let ucx_inc_dir = ucx_env.join("include");
    let ucx_lib_dir_base = ucx_env.join("lib");

    println!("cargo:root={}", out_path.display());
    println!(
        "cargo:rustc-link-search=native={}",
        ucx_lib_dir_base.display()
    );
    println!(
        "cargo:rustc-link-arg=-Wl,-rpath,{}",
        ucx_lib_dir_base.display()
    );
    println!("cargo:rustc-link-lib=ucp");
    println!("cargo:rustc-link-lib=ibverbs");
    println!("cargo:rustc-link-lib=rdmacm");
    // println!("cargo:rustc-link-lib=pmi_simple");

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg(format!("-I{}", ucx_inc_dir.display()))
        .allowlist_item("uc[mpst].*")
        .allowlist_item("UC[MPST].*")
        .rustified_enum(".*")
        .bitfield_enum("ucp_feature")
        .bitfield_enum(".*_field")
        .bitfield_enum(".*_flags(_t)?")
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to src/bindings.rs file.
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

fn main() {
    build_bindings();
}
