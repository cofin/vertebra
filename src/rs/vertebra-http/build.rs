fn main() {
    println!("cargo::rustc-check-cfg=cfg(Py_GIL_DISABLED)");
    if std::env::var_os("CARGO_FEATURE_PYTHON").is_some() {
        pyo3_build_config::use_pyo3_cfgs();
        #[cfg(unix)]
        if let Some(lib_dir) = pyo3_build_config::get().lib_dir() {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
        }
    }
}
