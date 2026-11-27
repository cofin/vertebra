//! Bundler -- Main bundler implementation using OXC

use crate::config::{BundleConfig, BundleDevConfig, BundleOutput, BundleResult};
use crate::errors::{BundlerError, Result};
use crate::hmr::{HmrEvent, SourceWatcher};
use crossbeam_channel::Receiver;
use oxc_allocator::Allocator;
use oxc_codegen::{Codegen, CodegenOptions};
use oxc_parser::Parser;
use oxc_semantic::SemanticBuilder;
use oxc_span::SourceType;
use oxc_transformer::{TransformOptions, Transformer};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyType;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Main bundler class exposed to Python via PyO3
#[derive(Default)]
#[pyclass(name = "Bundler", immutable_type)]
pub struct Bundler {
    config: Option<BundleConfig>,
    dev_config: Option<BundleDevConfig>,
    watcher: Option<SourceWatcher>,
    rx: Option<Receiver<HmrEvent>>,
    hmr_port: Option<u16>,
    is_dev_mode: bool,
}

#[pymethods]
impl Bundler {
    /// Create a new Bundler instance, optionally bound to a build and dev configuration.
    #[new]
    #[pyo3(signature = (config=None, *, dev_config=None))]
    pub fn new(config: Option<BundleConfig>, dev_config: Option<BundleDevConfig>) -> Self {
        Self {
            config,
            dev_config,
            ..Self::default()
        }
    }

    /// Construct a Bundler bound to a BundleConfig and optional BundleDevConfig.
    #[classmethod]
    #[pyo3(signature = (config, *, dev_config=None))]
    pub fn from_config(
        _cls: &Bound<'_, PyType>,
        config: BundleConfig,
        dev_config: Option<BundleDevConfig>,
    ) -> Self {
        Self {
            config: Some(config),
            dev_config,
            ..Self::default()
        }
    }

    /// Return the bound production bundle configuration, if any.
    #[getter]
    pub fn config(&self) -> Option<BundleConfig> {
        self.config.clone()
    }

    /// Return the bound dev watcher configuration, if any.
    #[getter]
    pub fn dev_config(&self) -> Option<BundleDevConfig> {
        self.dev_config.clone()
    }

    /// Enter a context manager scope returning this Bundler instance.
    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Exit a context manager scope, stopping any active dev watcher.
    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    pub fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.stop_dev()?;
        Ok(false)
    }

    /// Build a bundle from the bound configuration or an explicit override.
    #[pyo3(signature = (config=None))]
    pub fn build(&self, py: Python<'_>, config: Option<&BundleConfig>) -> PyResult<BundleResult> {
        let effective = config.or(self.config.as_ref()).ok_or_else(|| {
            PyValueError::new_err(
                "Bundler.build() requires a BundleConfig passed to Bundler(config) or build(config)",
            )
        })?;
        let result = py.detach(|| self.build_internal(effective))?;
        Ok(result)
    }

    /// Start development mode with HMR using the bound dev configuration or an explicit override.
    #[pyo3(signature = (config=None))]
    pub fn start_dev(&mut self, _py: Python<'_>, config: Option<&BundleDevConfig>) -> PyResult<()> {
        if self.is_dev_mode {
            return Err(BundlerError::Watcher("Dev mode already started".to_string()).into());
        }

        let effective = config.or(self.dev_config.as_ref()).ok_or_else(|| {
            PyValueError::new_err(
                "Bundler.start_dev() requires a BundleDevConfig passed to Bundler(dev_config=...) or start_dev(config)",
            )
        })?;

        let watch_dirs: Vec<PathBuf> = effective.watch_dirs.iter().map(PathBuf::from).collect();

        let (mut watcher, rx) = SourceWatcher::new(
            watch_dirs,
            effective.extensions.clone(),
            effective.debounce_ms,
        )?;

        watcher.start()?;

        self.watcher = Some(watcher);
        self.rx = Some(rx);
        self.hmr_port = Some(effective.hmr_port);
        self.is_dev_mode = true;

        Ok(())
    }

    /// Stop development mode
    pub fn stop_dev(&mut self) -> PyResult<()> {
        if !self.is_dev_mode {
            return Ok(());
        }

        if let Some(ref mut w) = self.watcher {
            w.stop()?;
        }

        self.watcher = None;
        self.rx = None;
        self.hmr_port = None;
        self.is_dev_mode = false;

        Ok(())
    }

    /// Drain updated file paths from the dev watcher receiver.
    #[pyo3(signature = (timeout_ms=None))]
    pub fn poll_events(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<Vec<String>> {
        let rx = match &self.rx {
            Some(rx) => rx.clone(),
            None => return Ok(Vec::new()),
        };

        py.detach(|| {
            let mut paths = Vec::new();
            if let Some(ms) = timeout_ms
                && ms > 0
                && let Ok(HmrEvent::Update { path, .. }) =
                    rx.recv_timeout(Duration::from_millis(ms))
            {
                paths.push(path);
            }
            while let Ok(evt) = rx.try_recv() {
                if let HmrEvent::Update { path, .. } = evt {
                    paths.push(path);
                }
            }
            Ok(paths)
        })
    }

    /// Transform a single TypeScript/JavaScript file
    pub fn transform_file(&self, py: Python<'_>, path: &str, source: &str) -> PyResult<String> {
        let result = py.detach(|| transform_internal(path, source, false, "es2022", false))?;
        Ok(result.0)
    }

    /// Parse a file and return diagnostics
    pub fn check_file(&self, py: Python<'_>, path: &str, source: &str) -> PyResult<Vec<String>> {
        py.detach(|| {
            let allocator = Allocator::default();
            let source_type = SourceType::from_path(path).unwrap_or_default();
            let ret = Parser::new(&allocator, source, source_type).parse();
            let diagnostics: Vec<String> = ret.diagnostics.iter().map(|e| e.to_string()).collect();
            Ok(diagnostics)
        })
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
}

impl Bundler {
    /// Internal build implementation
    fn build_internal(&self, config: &BundleConfig) -> Result<BundleResult> {
        let start = Instant::now();
        let mut outputs = Vec::new();
        let warnings = Vec::new();

        let entry_path = config.entry_path();
        let out_dir = config.output_path();

        // Ensure output directory exists
        fs::create_dir_all(&out_dir)?;

        // Read and transform entry file
        let source = fs::read_to_string(&entry_path).map_err(BundlerError::Io)?;

        let (mut final_code, sourcemap) = transform_internal(
            entry_path.to_string_lossy().as_ref(),
            &source,
            config.minify,
            &config.target,
            config.sourcemap,
        )?;

        // Determine output filename
        let entry_stem = entry_path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "bundle".to_string());
        let output_name = format!("{}.js", entry_stem);
        let output_path = out_dir.join(&output_name);

        if config.sourcemap {
            let map_name = format!("{}.js.map", entry_stem);
            let map_path = out_dir.join(&map_name);
            if let Some(map_str) = sourcemap {
                fs::write(&map_path, &map_str)?;
                outputs.push(BundleOutput::new(map_name, map_str.len() as u64, false));
                final_code.push_str(&format!("\n//# sourceMappingURL={}.js.map\n", entry_stem));
            }
        }

        // Write output
        fs::write(&output_path, &final_code)?;

        outputs.push(BundleOutput::new(
            output_name,
            final_code.len() as u64,
            true,
        ));

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(BundleResult::new(outputs, duration_ms, warnings))
    }
}

/// Transform TypeScript/JavaScript to ES target, stripping type annotations
fn transform_internal(
    path: &str,
    source: &str,
    minify: bool,
    target: &str,
    sourcemap: bool,
) -> Result<(String, Option<String>)> {
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

    // Run the OXC transformer using configured target
    let transform_options =
        TransformOptions::from_target(target).map_err(BundlerError::Transform)?;
    let _transform_result = Transformer::new(&allocator, Path::new(path), &transform_options)
        .build_with_scoping(scoping, &mut ret.program);

    // Generate output code
    let source_map_path = if sourcemap {
        Some(PathBuf::from(
            Path::new(path).file_name().unwrap_or_default(),
        ))
    } else {
        None
    };

    let codegen = Codegen::new()
        .with_options(CodegenOptions {
            minify,
            source_map_path,
            ..CodegenOptions::default()
        })
        .build(&ret.program);

    let map_str = codegen.map.map(|m| m.to_json_string());
    Ok((codegen.code, map_str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_transform_typescript() {
        let ts_source = "const greeting: string = 'hello';\nfunction add(a: number, b: number): number { return a + b; }\n";
        let result = transform_internal("test.ts", ts_source, false, "es2022", false);
        assert!(result.is_ok(), "Transform failed: {:?}", result.err());
        let (output, _) = result.unwrap();
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
        let result = transform_internal("test.js", source, true, "es2022", false);
        assert!(result.is_ok(), "Minify failed: {:?}", result.err());
        let (output, _) = result.unwrap();
        // Minified code should be shorter (less whitespace)
        assert!(output.len() < source.len(), "Minified output not shorter");
        assert!(output.contains("greeting"));
        assert!(output.contains("hello"));
    }

    #[test]
    fn test_build_writes_output_and_sourcemap() {
        let tmp = TempDir::new().unwrap();
        let entry_path = tmp.path().join("main.ts");
        let out_dir = tmp.path().join("dist");

        fs::write(&entry_path, "export const x: number = 42;\n").unwrap();

        let config = BundleConfig {
            entry: entry_path.to_string_lossy().to_string(),
            outdir: out_dir.to_string_lossy().to_string(),
            minify: false,
            sourcemap: true,
            target: "es2022".to_string(),
        };

        let bundler = Bundler::default();
        let result = bundler.build_internal(&config);
        assert!(result.is_ok(), "Build failed: {:?}", result.err());

        let build_result = result.unwrap();
        assert_eq!(build_result.outputs.len(), 2);
        assert!(
            build_result
                .outputs
                .iter()
                .any(|o| o.path == "main.js" && o.is_entry)
        );
        assert!(
            build_result
                .outputs
                .iter()
                .any(|o| o.path == "main.js.map" && !o.is_entry)
        );

        // Verify JS output file was written and contains sourcemap reference
        let output_file = out_dir.join("main.js");
        assert!(output_file.exists(), "Output file not written");
        let content = fs::read_to_string(&output_file).unwrap();
        assert!(content.contains("42"));
        assert!(!content.contains(": number"));
        assert!(content.contains("//# sourceMappingURL=main.js.map"));

        // Verify map file was written
        let map_file = out_dir.join("main.js.map");
        assert!(map_file.exists(), "Sourcemap file not written");
    }

    #[test]
    fn test_invalid_target_returns_error() {
        let ts_source = "const greeting: string = 'hello';\n";
        let result = transform_internal("test.ts", ts_source, false, "invalid_target_12345", false);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), BundlerError::Transform(_)));
    }
}
