//! RolldownBundler -- Main bundler implementation using OXC

use crate::config::{BuildResult, BundleConfig, DevConfig, OutputFile};
use crate::errors::{BundlerError, Result};
use crate::hmr::{HmrEvent, HmrWatcher};
use crate::ssr::SsrPool;
use oxc_allocator::Allocator;
use oxc_codegen::{Codegen, CodegenOptions};
use oxc_parser::Parser;
use oxc_semantic::SemanticBuilder;
use oxc_span::SourceType;
use oxc_transformer::{TransformOptions, Transformer};
use pyo3::prelude::*;
use pyo3::types::PyList;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;

/// Main bundler class exposed to Python via PyO3
#[pyclass]
pub struct RolldownBundler {
    /// Tokio runtime is retained so futures spawned during dev-mode watching
    /// still have an executor even though we never call into it directly here.
    #[allow(dead_code)]
    runtime: Runtime,
    watcher: Option<HmrWatcher>,
    hmr_rx: Option<broadcast::Receiver<HmrEvent>>,
    hmr_port: Option<u16>,
    is_dev_mode: bool,
    ssr_pool: Option<SsrPool>,
}

#[pymethods]
impl RolldownBundler {
    /// Create a new RolldownBundler instance
    #[new]
    pub fn new() -> PyResult<Self> {
        let runtime = Runtime::new().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create runtime: {}", e))
        })?;

        Ok(Self {
            runtime,
            watcher: None,
            hmr_rx: None,
            hmr_port: None,
            is_dev_mode: false,
            ssr_pool: None,
        })
    }

    /// Build a bundle from the given configuration
    pub fn build(&self, py: Python<'_>, config: &BundleConfig) -> PyResult<BuildResult> {
        py.detach(|| {
            self.build_internal(config)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Start development mode with HMR
    pub fn start_dev(&mut self, _py: Python<'_>, config: &DevConfig) -> PyResult<()> {
        if self.is_dev_mode {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Dev mode already started",
            ));
        }

        let watch_dirs: Vec<PathBuf> = config.watch_dirs.iter().map(PathBuf::from).collect();

        let (watcher, rx) =
            HmrWatcher::new(watch_dirs, config.extensions.clone(), config.debounce_ms)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        self.watcher = Some(watcher);
        self.hmr_rx = Some(rx);
        self.hmr_port = Some(config.hmr_port);
        self.is_dev_mode = true;

        // Start watching
        if let Some(ref mut w) = self.watcher {
            w.start()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        }

        Ok(())
    }

    /// Stop development mode
    pub fn stop_dev(&mut self) -> PyResult<()> {
        if !self.is_dev_mode {
            return Ok(());
        }

        if let Some(ref mut w) = self.watcher {
            w.stop()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        }

        self.watcher = None;
        self.hmr_rx = None;
        self.hmr_port = None;
        self.is_dev_mode = false;

        Ok(())
    }

    /// Transform a single TypeScript/JavaScript file
    pub fn transform_file(&self, py: Python<'_>, path: &str, source: &str) -> PyResult<String> {
        let result = py.detach(|| transform_internal(path, source).map_err(|e| e.to_string()));
        result.map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }

    /// Parse a file and return diagnostics
    pub fn check_file(&self, path: &str, source: &str) -> PyResult<Vec<String>> {
        let allocator = Allocator::default();
        let source_type = SourceType::from_path(path).unwrap_or_default();

        let ret = Parser::new(&allocator, source, source_type).parse();

        let diagnostics: Vec<String> = ret.diagnostics.iter().map(|e| e.to_string()).collect();

        Ok(diagnostics)
    }

    /// Notify connected clients of a type definition change
    #[pyo3(signature = (version=None))]
    pub fn notify_type_change(&self, version: Option<String>) -> PyResult<()> {
        if let Some(ref w) = self.watcher {
            w.notify_type_change(&version.unwrap_or_else(|| "latest".to_string()));
        }
        Ok(())
    }

    /// Trigger a full reload for connected clients
    pub fn notify_reload(&self) -> PyResult<()> {
        if let Some(ref w) = self.watcher {
            w.notify_reload();
        }
        Ok(())
    }

    /// Get the HMR WebSocket URL if in dev mode
    pub fn hmr_url(&self) -> Option<String> {
        self.hmr_port
            .map(|port| format!("ws://localhost:{}/hmr", port))
    }

    /// Check if dev mode is active
    pub fn is_dev_mode(&self) -> bool {
        self.is_dev_mode
    }

    /// Initialize the SSR pool with a pre-built JS bundle.
    ///
    /// `bundle_path` is the path to the SSR bundle on disk (must define
    /// `globalThis.__ssr_render`). `pool_size` controls the number of
    /// pre-warmed QuickJS contexts.
    pub fn init_ssr(&mut self, bundle_path: &str, pool_size: usize) -> PyResult<()> {
        let pool = SsrPool::new(bundle_path, pool_size)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        self.ssr_pool = Some(pool);
        Ok(())
    }

    /// Render a component via SSR.
    ///
    /// Returns a Python dict `{"head": List[str], "body": str}`.
    /// The SSR pool must be initialized first via `init_ssr()`.
    pub fn ssr_render(
        &self,
        py: Python<'_>,
        component: &str,
        page_json: &str,
    ) -> PyResult<HashMap<String, Py<PyAny>>> {
        let pool = self.ssr_pool.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("SSR not initialized. Call init_ssr() first.")
        })?;

        let result = py
            .detach(|| pool.render(component, page_json).map_err(|e| e.to_string()))
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

        // Convert SsrResult to Python dict.
        let mut map = HashMap::new();
        let head_list = PyList::new(py, result.head.iter().map(|s| s.as_str()))?;
        map.insert("head".to_string(), head_list.unbind().into_any());
        map.insert(
            "body".to_string(),
            result.body.into_pyobject(py)?.unbind().into_any(),
        );
        Ok(map)
    }

    /// Invalidate SSR cache for the given file paths.
    ///
    /// Currently clears the entire render cache and marks all contexts
    /// as stale. A future version may do selective invalidation.
    pub fn ssr_invalidate(&self, _paths: Vec<String>) -> PyResult<()> {
        if let Some(pool) = &self.ssr_pool {
            pool.invalidate();
        }
        Ok(())
    }

    /// Shut down the SSR pool, releasing all QuickJS contexts.
    pub fn ssr_shutdown(&mut self) -> PyResult<()> {
        if let Some(pool) = self.ssr_pool.as_mut() {
            pool.shutdown();
        }
        self.ssr_pool = None;
        Ok(())
    }
}

impl RolldownBundler {
    /// Internal build implementation
    fn build_internal(&self, config: &BundleConfig) -> Result<BuildResult> {
        let start = Instant::now();
        let mut outputs = Vec::new();
        let warnings = Vec::new();

        let entry_path = config.entry_path();
        let out_dir = config.output_path();

        // Ensure output directory exists
        fs::create_dir_all(&out_dir)?;

        // Read and transform entry file
        let source = fs::read_to_string(&entry_path).map_err(BundlerError::Io)?;

        let transformed = transform_internal(entry_path.to_string_lossy().as_ref(), &source)?;

        // Apply minification if requested
        let final_code = if config.minify {
            minify_code(&transformed)?
        } else {
            transformed
        };

        // Determine output filename
        let entry_stem = entry_path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "bundle".to_string());
        let output_name = format!("{}.js", entry_stem);
        let output_path = out_dir.join(&output_name);

        // Write output
        fs::write(&output_path, &final_code)?;

        outputs.push(OutputFile::new(output_name, final_code.len() as u64, true));

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(BuildResult::new(outputs, duration_ms, warnings))
    }
}

impl Default for RolldownBundler {
    fn default() -> Self {
        Self::new().expect("Failed to create default bundler")
    }
}

/// Transform TypeScript/JavaScript to ES target, stripping type annotations
fn transform_internal(path: &str, source: &str) -> Result<String> {
    let allocator = Allocator::default();
    let source_type = SourceType::from_path(path).unwrap_or_default();

    let mut ret = Parser::new(&allocator, source, source_type).parse();

    if !ret.diagnostics.is_empty() {
        return Err(BundlerError::Parse {
            path: path.to_string(),
            message: ret
                .diagnostics
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("; "),
        });
    }

    // Run semantic analysis to produce scoping info needed by the transformer
    let semantic_ret = SemanticBuilder::new().build(&ret.program);
    let scoping = semantic_ret.semantic.into_scoping();

    // Run the OXC transformer to strip TypeScript types and apply ES target transforms
    let transform_options = TransformOptions::default();
    let _transform_result =
        Transformer::new(&allocator, std::path::Path::new(path), &transform_options)
            .build_with_scoping(scoping, &mut ret.program);

    // Generate output code
    let codegen = Codegen::new()
        .with_options(CodegenOptions {
            minify: false,
            ..Default::default()
        })
        .build(&ret.program);

    Ok(codegen.code)
}

/// Simple minification via OXC codegen with minify flag
fn minify_code(code: &str) -> Result<String> {
    let allocator = Allocator::default();
    let source_type = SourceType::default().with_module(true);

    let ret = Parser::new(&allocator, code, source_type).parse();

    if !ret.diagnostics.is_empty() {
        return Err(BundlerError::Transform("Minification parse error".into()));
    }

    let codegen = Codegen::new()
        .with_options(CodegenOptions {
            minify: true,
            ..Default::default()
        })
        .build(&ret.program);

    Ok(codegen.code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_transform_typescript() {
        let ts_source = "const greeting: string = 'hello';\nfunction add(a: number, b: number): number { return a + b; }\n";
        let result = transform_internal("test.ts", ts_source);
        assert!(result.is_ok(), "Transform failed: {:?}", result.err());
        let output = result.unwrap();
        // TypeScript type annotations should be stripped
        assert!(
            !output.contains(": string"),
            "Type annotation not stripped: {}",
            output
        );
        assert!(
            !output.contains(": number"),
            "Type annotation not stripped: {}",
            output
        );
        // But the runtime code should remain
        assert!(output.contains("greeting"));
        assert!(output.contains("hello"));
        assert!(output.contains("return a + b"));
    }

    #[test]
    fn test_minify_output() {
        let source = "const   greeting   =   'hello';\n\nconst   x   =   42;\n";
        let result = minify_code(source);
        assert!(result.is_ok(), "Minify failed: {:?}", result.err());
        let output = result.unwrap();
        // Minified code should be shorter (less whitespace)
        assert!(output.len() < source.len(), "Minified output not shorter");
        assert!(output.contains("greeting"));
        assert!(output.contains("hello"));
    }

    #[test]
    fn test_build_writes_output() {
        let tmp = TempDir::new().unwrap();
        let entry_path = tmp.path().join("main.ts");
        let out_dir = tmp.path().join("dist");

        fs::write(&entry_path, "export const x: number = 42;\n").unwrap();

        let config = BundleConfig {
            entry: entry_path.to_string_lossy().to_string(),
            outdir: out_dir.to_string_lossy().to_string(),
            minify: false,
            ..BundleConfig::default()
        };

        let bundler = RolldownBundler::default();
        let result = bundler.build_internal(&config);
        assert!(result.is_ok(), "Build failed: {:?}", result.err());

        let build_result = result.unwrap();
        assert_eq!(build_result.outputs.len(), 1);
        assert_eq!(build_result.outputs[0].path, "main.js");
        assert!(build_result.outputs[0].is_entry);

        // Verify file was written
        let output_file = out_dir.join("main.js");
        assert!(output_file.exists(), "Output file not written");
        let content = fs::read_to_string(&output_file).unwrap();
        assert!(content.contains("42"));
        // Type annotation should be stripped
        assert!(!content.contains(": number"));
    }
}
