//! QuickJS SSR pool for server-side rendering with per-slot runtimes and atomic generation reload.
//!
//! Each `SsrSlot` owns an independent QuickJS `(Runtime, Context)` pair so concurrent
//! worker threads never contend on a shared `Runtime` lock and stale slots are rebuilt
//! in-place when `invalidate()` increments the pool generation counter.

use crate::errors::{BundlerError, Result};
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use rquickjs::function::Rest;
use rquickjs::{CatchResultExt, Context, Function, Object, Runtime, String as JsString, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

/// Result of an SSR render call.
#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub struct SsrResult {
    /// Head tags emitted by the component (e.g. `<title>`, `<meta>`).
    #[pyo3(get)]
    pub head: Vec<String>,
    /// Rendered HTML body string.
    #[pyo3(get)]
    pub body: String,
}

/// Cached render entry with TTL tracking.
struct CachedRender {
    result: SsrResult,
    created_at: Instant,
    generation: u64,
}

/// Independent QuickJS runtime + context pair owned by a single pool slot.
struct SlotEngine {
    #[allow(dead_code)]
    runtime: Runtime,
    ctx: Context,
    generation: u64,
}

// Safety: Each `SsrSlot` is checked out exclusively via `ArrayQueue<usize>` before
// locking its internal `Mutex<Option<SlotEngine>>`, so a `Runtime`/`Context` is
// never accessed concurrently by two threads at the same time.
unsafe impl Send for SlotEngine {}

struct SsrSlot {
    engine: Mutex<Option<SlotEngine>>,
}

/// Pool of independent QuickJS runtimes and contexts for SSR rendering.
#[pyclass]
pub struct SsrPool {
    bundle_path: PathBuf,
    bundle_source: RwLock<String>,
    generation: AtomicU64,
    slots: Vec<SsrSlot>,
    available: ArrayQueue<usize>,
    cache: DashMap<u64, CachedRender>,
    cache_ttl: Duration,
}

impl SsrPool {
    /// Create a new SSR pool with `pool_size` independent QuickJS runtime slots.
    pub fn new(bundle_path: &str, pool_size: usize) -> Result<Self> {
        let resolved_size = pool_size.max(1);
        let path_buf = PathBuf::from(bundle_path);
        let bundle_source = std::fs::read_to_string(&path_buf).map_err(BundlerError::Io)?;

        let mut slots = Vec::with_capacity(resolved_size);
        let available = ArrayQueue::new(resolved_size);

        for i in 0..resolved_size {
            let engine = Self::create_slot_engine(&bundle_source, 0)?;
            slots.push(SsrSlot {
                engine: Mutex::new(Some(engine)),
            });
            available
                .push(i)
                .expect("ArrayQueue capacity matches resolved_size");
        }

        Ok(Self {
            bundle_path: path_buf,
            bundle_source: RwLock::new(bundle_source),
            generation: AtomicU64::new(0),
            slots,
            available,
            cache: DashMap::new(),
            cache_ttl: Duration::from_secs(60),
        })
    }

    fn create_slot_engine(source: &str, generation: u64) -> Result<SlotEngine> {
        let runtime = Runtime::new()
            .map_err(|e| BundlerError::Ssr(format!("Failed to create QuickJS runtime: {e}")))?;
        runtime.set_memory_limit(256 * 1024 * 1024);

        let ctx = Context::full(&runtime)
            .map_err(|e| BundlerError::Ssr(format!("Failed to create context: {e}")))?;

        ctx.with(|ctx| {
            let globals = ctx.globals();

            let console = Object::new(ctx.clone())
                .map_err(|e| BundlerError::Ssr(format!("Failed to create console object: {e}")))?;

            let log_fn: Function = Function::new(ctx.clone(), |args: Rest<Value<'_>>| {
                let parts: Vec<std::string::String> = args
                    .0
                    .iter()
                    .map(|v: &Value<'_>| {
                        v.as_string()
                            .and_then(|s: &JsString<'_>| s.to_string().ok())
                            .unwrap_or_else(|| format!("{v:?}"))
                    })
                    .collect();
                eprintln!("[ssr] {}", parts.join(" "));
            })
            .map_err(|e| BundlerError::Ssr(format!("Failed to create console.log: {e}")))?;

            console
                .set("log", log_fn)
                .map_err(|e| BundlerError::Ssr(format!("Failed to set console.log: {e}")))?;

            globals
                .set("console", console)
                .map_err(|e| BundlerError::Ssr(format!("Failed to set console global: {e}")))?;

            ctx.eval::<(), _>(source.as_bytes())
                .catch(&ctx)
                .map_err(|e| BundlerError::Ssr(format!("Bundle eval failed: {e}")))?;

            Ok::<_, BundlerError>(())
        })?;

        Ok(SlotEngine {
            runtime,
            ctx,
            generation,
        })
    }

    /// Render a component to HTML, rebuilding the checked-out slot if its generation is stale.
    pub fn render(&self, component: &str, page_json: &str) -> Result<SsrResult> {
        let current_gen = self.generation.load(Ordering::Acquire);
        let cache_key = self.cache_key(component, page_json);
        if let Some(cached) = self.cache.get(&cache_key)
            && cached.generation == current_gen
            && cached.created_at.elapsed() < self.cache_ttl
        {
            return Ok(cached.result.clone());
        }

        let idx = self.available.pop().ok_or(BundlerError::PoolExhausted)?;
        let render_res = (|| -> Result<SsrResult> {
            let mut slot_guard = self.slots[idx]
                .engine
                .lock()
                .map_err(|_| BundlerError::Ssr("SSR slot mutex poisoned".to_string()))?;

            let needs_rebuild = match slot_guard.as_ref() {
                Some(eng) => eng.generation != current_gen,
                None => true,
            };
            if needs_rebuild {
                let latest_source = self
                    .bundle_source
                    .read()
                    .map_err(|_| BundlerError::Ssr("SSR bundle_source lock poisoned".to_string()))?
                    .clone();
                let fresh_engine = Self::create_slot_engine(&latest_source, current_gen)?;
                *slot_guard = Some(fresh_engine);
            }

            let eng = slot_guard
                .as_ref()
                .ok_or_else(|| BundlerError::Ssr("SSR slot unavailable".to_string()))?;

            eng.ctx.with(|ctx| {
                let globals = ctx.globals();

                let render_fn: Function = globals.get("__ssr_render").catch(&ctx).map_err(|e| {
                    BundlerError::Ssr(format!(
                        "__ssr_render not found in SSR bundle. \
                             The bundle must set globalThis.__ssr_render = \
                             function(component, props) {{ ... }}. Error: {e}"
                    ))
                })?;

                let json_obj: Object = globals
                    .get("JSON")
                    .catch(&ctx)
                    .map_err(|e| BundlerError::Ssr(format!("JSON global not available: {e}")))?;

                let parse_fn: Function = json_obj
                    .get("parse")
                    .catch(&ctx)
                    .map_err(|e| BundlerError::Ssr(format!("JSON.parse not available: {e}")))?;

                let parsed_props: Value = parse_fn
                    .call((page_json,))
                    .catch(&ctx)
                    .map_err(|e| BundlerError::Ssr(format!("JSON.parse failed: {e}")))?;

                let result: Object = render_fn
                    .call((component, parsed_props))
                    .catch(&ctx)
                    .map_err(|e| BundlerError::Ssr(format!("__ssr_render failed: {e}")))?;

                let head_val: Value = result
                    .get("head")
                    .catch(&ctx)
                    .map_err(|e| BundlerError::Ssr(format!("result.head missing: {e}")))?;

                let mut head = Vec::new();
                if let Some(arr) = head_val.as_array() {
                    for i in 0..arr.len() {
                        if let Ok(val) = arr.get::<Value>(i)
                            && let Some(s) = val.as_string()
                            && let Ok(s) = s.to_string()
                        {
                            head.push(s);
                        }
                    }
                }

                let body: String = result
                    .get("body")
                    .catch(&ctx)
                    .map_err(|e| BundlerError::Ssr(format!("result.body missing: {e}")))?;

                Ok::<_, BundlerError>(SsrResult { head, body })
            })
        })();

        self.available
            .push(idx)
            .expect("ArrayQueue slot was popped, push must succeed");

        let render_result = render_res?;
        self.cache.insert(
            cache_key,
            CachedRender {
                result: render_result.clone(),
                created_at: Instant::now(),
                generation: current_gen,
            },
        );

        Ok(render_result)
    }

    /// Reload bundle source from disk (if modified) and advance the generation counter so slots rebuild on next checkout.
    pub fn invalidate(&self) {
        if let Ok(fresh_source) = std::fs::read_to_string(&self.bundle_path)
            && let Ok(mut guard) = self.bundle_source.write()
        {
            *guard = fresh_source;
        }
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.cache.clear();
    }

    /// Shut down the pool: clear the cache and drop all slot engines.
    pub fn shutdown(&mut self) {
        self.cache.clear();
        self.slots.clear();
    }

    /// Return the current generation number of the SSR pool.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn cache_key(&self, component: &str, page_json: &str) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in component
            .bytes()
            .chain(b":".iter().copied())
            .chain(page_json.bytes())
        {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        hash
    }
}

#[pymethods]
impl SsrPool {
    #[new]
    #[pyo3(signature = (bundle_path, pool_size=4))]
    fn py_new(bundle_path: &str, pool_size: usize) -> PyResult<Self> {
        Self::new(bundle_path, pool_size).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    #[pyo3(name = "render")]
    fn py_render(&self, py: Python<'_>, component: &str, page_json: &str) -> PyResult<SsrResult> {
        py.detach(|| self.render(component, page_json))
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    #[pyo3(name = "invalidate")]
    fn py_invalidate(&self) {
        self.invalidate();
    }

    #[getter]
    #[pyo3(name = "generation")]
    fn py_generation(&self) -> u64 {
        self.generation()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_ssr_bundle(dir: &std::path::Path) -> std::path::PathBuf {
        let path = dir.join("ssr-bundle.js");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(
            f,
            r#"
            globalThis.__ssr_render = function(component, props) {{
                return {{
                    head: ["<title>" + component + "</title>"],
                    body: "<div id=\"app\">" + component + ": " + JSON.stringify(props) + "</div>"
                }};
            }};
        "#
        )
        .unwrap();
        path
    }

    #[test]
    fn test_ssr_pool_creation() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = write_ssr_bundle(dir.path());
        let pool = SsrPool::new(bundle.to_str().unwrap(), 2).unwrap();
        assert_eq!(pool.slots.len(), 2);
    }

    #[test]
    fn test_ssr_render_basic() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = write_ssr_bundle(dir.path());
        let pool = SsrPool::new(bundle.to_str().unwrap(), 1).unwrap();

        let result = pool.render("Dashboard", r#"{"user":"test"}"#).unwrap();
        assert!(result.body.contains("Dashboard"));
        assert!(result.body.contains("test"));
        assert_eq!(result.head.len(), 1);
        assert!(result.head[0].contains("<title>Dashboard</title>"));
    }

    #[test]
    fn test_ssr_render_cached() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = write_ssr_bundle(dir.path());
        let pool = SsrPool::new(bundle.to_str().unwrap(), 1).unwrap();

        let r1 = pool.render("App", r#"{"a":1}"#).unwrap();
        let r2 = pool.render("App", r#"{"a":1}"#).unwrap();
        assert_eq!(r1.body, r2.body);
        assert_eq!(r1.head, r2.head);
    }

    #[test]
    fn test_ssr_invalidate_rebuilds_slot_from_updated_bundle() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = write_ssr_bundle(dir.path());
        let pool = SsrPool::new(bundle.to_str().unwrap(), 1).unwrap();

        let r1 = pool.render("App", r#"{}"#).unwrap();
        assert!(r1.body.contains("<div id=\"app\">App:"));
        assert!(!pool.cache.is_empty());

        std::fs::write(
            &bundle,
            r#"
            globalThis.__ssr_render = function(component, props) {
                return {
                    head: ["<title>V2:" + component + "</title>"],
                    body: "<main data-v2=\"true\">" + component + "</main>"
                };
            };
            "#,
        )
        .unwrap();

        pool.invalidate();
        assert!(pool.cache.is_empty());
        assert_eq!(pool.generation(), 1);

        let r2 = pool.render("App", r#"{}"#).unwrap();
        assert!(r2.body.contains("data-v2=\"true\""));
        assert_eq!(r2.head[0], "<title>V2:App</title>");
    }

    #[test]
    fn test_ssr_cache_key_different_for_different_input() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = write_ssr_bundle(dir.path());
        let pool = SsrPool::new(bundle.to_str().unwrap(), 1).unwrap();
        let k1 = pool.cache_key("A", "{}");
        let k2 = pool.cache_key("B", "{}");
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_ssr_missing_render_function() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.js");
        std::fs::write(&path, "// no __ssr_render defined").unwrap();
        let pool = SsrPool::new(path.to_str().unwrap(), 1).unwrap();
        let result = pool.render("App", "{}");
        assert!(result.is_err());
    }

    #[test]
    fn test_ssr_shutdown_clears_state() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = write_ssr_bundle(dir.path());
        let mut pool = SsrPool::new(bundle.to_str().unwrap(), 2).unwrap();

        pool.render("App", r#"{}"#).unwrap();
        assert!(!pool.cache.is_empty());
        assert_eq!(pool.slots.len(), 2);

        pool.shutdown();
        assert!(pool.cache.is_empty());
        assert!(pool.slots.is_empty());
    }

    #[test]
    fn test_ssr_pool_multiple_contexts() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = write_ssr_bundle(dir.path());
        let pool = SsrPool::new(bundle.to_str().unwrap(), 4).unwrap();

        for i in 0..8 {
            let component = format!("Component{i}");
            let props = format!(r#"{{"idx":{i}}}"#);
            let result = pool.render(&component, &props).unwrap();
            assert!(result.body.contains(&component));
        }
    }
}
