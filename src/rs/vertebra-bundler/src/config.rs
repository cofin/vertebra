//! Configuration types for the bundler

use pyo3::prelude::*;
use std::path::PathBuf;

/// Configuration for production builds
#[pyclass(immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct BundleConfig {
    /// Entry point file path
    #[pyo3(get, set)]
    pub entry: String,

    /// Output directory
    #[pyo3(get, set)]
    pub outdir: String,

    /// Enable minification
    #[pyo3(get, set)]
    pub minify: bool,

    /// Enable source maps
    #[pyo3(get, set)]
    pub sourcemap: bool,

    /// Target environment (es2020, es2022, esnext)
    #[pyo3(get, set)]
    pub target: String,
}

#[pymethods]
impl BundleConfig {
    #[new]
    #[pyo3(signature = (entry, outdir="dist".to_string(), minify=true, sourcemap=false, target="es2022".to_string()))]
    pub fn new(
        entry: String,
        outdir: String,
        minify: bool,
        sourcemap: bool,
        target: String,
    ) -> Self {
        Self {
            entry,
            outdir,
            minify,
            sourcemap,
            target,
        }
    }

    pub fn entry_path(&self) -> PathBuf {
        PathBuf::from(&self.entry)
    }

    pub fn output_path(&self) -> PathBuf {
        PathBuf::from(&self.outdir)
    }
}

impl Default for BundleConfig {
    fn default() -> Self {
        Self {
            entry: "src/main.ts".to_string(),
            outdir: "dist".to_string(),
            minify: true,
            sourcemap: false,
            target: "es2022".to_string(),
        }
    }
}

/// Configuration for development mode with HMR
#[pyclass(immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct DevConfig {
    /// Entry point file path
    #[pyo3(get, set)]
    pub entry: String,

    /// Watch directories
    #[pyo3(get, set)]
    pub watch_dirs: Vec<String>,

    /// HMR WebSocket port
    #[pyo3(get, set)]
    pub hmr_port: u16,

    /// File extensions to watch
    #[pyo3(get, set)]
    pub extensions: Vec<String>,

    /// Debounce delay in milliseconds
    #[pyo3(get, set)]
    pub debounce_ms: u64,
}

#[pymethods]
impl DevConfig {
    #[new]
    #[pyo3(signature = (entry, watch_dirs=vec!["src".to_string()], hmr_port=5173, extensions=vec!["ts".to_string(), "tsx".to_string(), "js".to_string(), "jsx".to_string()], debounce_ms=100))]
    pub fn new(
        entry: String,
        watch_dirs: Vec<String>,
        hmr_port: u16,
        extensions: Vec<String>,
        debounce_ms: u64,
    ) -> Self {
        Self {
            entry,
            watch_dirs,
            hmr_port,
            extensions,
            debounce_ms,
        }
    }
}

impl Default for DevConfig {
    fn default() -> Self {
        Self {
            entry: "src/main.ts".to_string(),
            watch_dirs: vec!["src".to_string()],
            hmr_port: 5173,
            extensions: vec![
                "ts".to_string(),
                "tsx".to_string(),
                "js".to_string(),
                "jsx".to_string(),
            ],
            debounce_ms: 100,
        }
    }
}

/// Build result returned from bundler
#[pyclass(immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct BuildResult {
    /// Output files generated
    #[pyo3(get)]
    pub outputs: Vec<OutputFile>,

    /// Build duration in milliseconds
    #[pyo3(get)]
    pub duration_ms: u64,

    /// Warnings generated during build
    #[pyo3(get)]
    pub warnings: Vec<String>,
}

#[pymethods]
impl BuildResult {
    #[new]
    pub fn new(outputs: Vec<OutputFile>, duration_ms: u64, warnings: Vec<String>) -> Self {
        Self {
            outputs,
            duration_ms,
            warnings,
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "BuildResult(outputs={}, duration_ms={}, warnings={})",
            self.outputs.len(),
            self.duration_ms,
            self.warnings.len()
        )
    }
}

/// Single output file from build
#[pyclass(immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct OutputFile {
    /// Output path relative to outdir
    #[pyo3(get)]
    pub path: String,

    /// File size in bytes
    #[pyo3(get)]
    pub size: u64,

    /// Whether this is an entry point
    #[pyo3(get)]
    pub is_entry: bool,
}

#[pymethods]
impl OutputFile {
    #[new]
    pub fn new(path: String, size: u64, is_entry: bool) -> Self {
        Self {
            path,
            size,
            is_entry,
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "OutputFile(path='{}', size={}, is_entry={})",
            self.path, self.size, self.is_entry
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bundle_config_defaults() {
        let config = BundleConfig::default();
        assert_eq!(config.entry, "src/main.ts");
        assert_eq!(config.outdir, "dist");
        assert!(config.minify);
        assert!(!config.sourcemap);
        assert_eq!(config.target, "es2022");
    }

    #[test]
    fn test_dev_config_defaults() {
        let config = DevConfig::default();
        assert_eq!(config.entry, "src/main.ts");
        assert_eq!(config.watch_dirs, vec!["src"]);
        assert_eq!(config.hmr_port, 5173);
        assert_eq!(config.extensions, vec!["ts", "tsx", "js", "jsx"]);
        assert_eq!(config.debounce_ms, 100);
    }
}
