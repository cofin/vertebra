//! QuickJS SSR pool for server-side rendering with per-slot runtimes and atomic generation reload.
//!
//! Each `SsrSlot` owns an independent QuickJS `(Runtime, Context)` pair so concurrent
//! worker threads never contend on a shared `Runtime` lock and stale slots are rebuilt
//! in-place when `invalidate()` increments the pool generation counter.

use crate::errors::{BundlerError, Result};
use pyo3::prelude::*;
use rquickjs::function::Rest;
use rquickjs::{CatchResultExt, Context, Function, Object, Runtime, String as JsString, Value};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

/// Result of an SSR render call.
#[pyclass(name = "SSRResult", immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct SSRResult {
    /// Head tags emitted by the component (e.g. `<title>`, `<meta>`).
    #[pyo3(get)]
    pub head: Vec<String>,
    /// Rendered HTML body string.
    #[pyo3(get)]
    pub body: String,
}

#[pymethods]
impl SSRResult {
    #[new]
    pub fn new(head: Vec<String>, body: String) -> Self {
        Self { head, body }
    }
}

/// Cached render entry with TTL tracking.
#[derive(Clone)]
struct CachedRender {
    result: SSRResult,
    created_at: Instant,
    generation: u64,
}

struct BoundedRenderCache {
    entries: HashMap<u64, CachedRender>,
    order: VecDeque<u64>,
    capacity: usize,
    ttl: Duration,
}

impl BoundedRenderCache {
    fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
            ttl,
        }
    }

    fn get(&mut self, key: u64, generation: u64) -> Option<SSRResult> {
        if let Some(entry) = self.entries.get(&key) {
            if entry.generation == generation && entry.created_at.elapsed() < self.ttl {
                return Some(entry.result.clone());
            }
            // Expired entry
            self.entries.remove(&key);
        }
        None
    }

    fn insert(&mut self, key: u64, result: SSRResult, generation: u64) {
        if self.entries.len() >= self.capacity && !self.entries.contains_key(&key) {
            // Evict expired first
            let mut evicted = false;
            let ttl = self.ttl;
            self.entries.retain(|_, v| {
                if !evicted && (v.generation != generation || v.created_at.elapsed() >= ttl) {
                    evicted = true;
                    false
                } else {
                    true
                }
            });

            // If still full, evict oldest from queue
            while self.entries.len() >= self.capacity {
                if let Some(oldest_key) = self.order.pop_front() {
                    self.entries.remove(&oldest_key);
                } else {
                    break;
                }
            }
        }

        self.entries.insert(
            key,
            CachedRender {
                result,
                created_at: Instant::now(),
                generation,
            },
        );
        self.order.push_back(key);
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Independent QuickJS runtime + context pair owned by a single pool slot.
struct SlotEngine {
    #[allow(dead_code)]
    runtime: Runtime,
    ctx: Context,
    generation: u64,
}

// Safety: Each `SsrSlot` is checked out exclusively via `Condvar` before
// locking its internal `Mutex<Option<SlotEngine>>`, so a `Runtime`/`Context` is
// never accessed concurrently by two threads at the same time.
unsafe impl Send for SlotEngine {}

struct SsrSlot {
    engine: Mutex<Option<SlotEngine>>,
}

/// Pool of independent QuickJS runtimes and contexts for SSR rendering.
#[pyclass(name = "SSRPool", immutable_type)]
pub struct SSRPool {
    bundle_path: PathBuf,
    bundle_source: RwLock<Arc<str>>,
    generation: AtomicU64,
    slots: Vec<SsrSlot>,
    available: Mutex<VecDeque<usize>>,
    condvar: Condvar,
    acquire_timeout: Duration,
    cache: Mutex<BoundedRenderCache>,
}

struct SlotLease<'a> {
    pool: &'a SSRPool,
    idx: usize,
}

impl Drop for SlotLease<'_> {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.pool.available.lock() {
            guard.push_back(self.idx);
            self.pool.condvar.notify_one();
        }
    }
}

impl SSRPool {
    /// Create a new SSR pool with `pool_size` independent QuickJS runtime slots.
    pub fn new(bundle_path: &str, pool_size: usize) -> Result<Self> {
        let resolved_size = pool_size.max(1);
        let path_buf = PathBuf::from(bundle_path);
        let source_raw = std::fs::read_to_string(&path_buf).map_err(BundlerError::Io)?;
        let bundle_source: Arc<str> = Arc::from(source_raw);

        let mut slots = Vec::with_capacity(resolved_size);
        let mut available = VecDeque::with_capacity(resolved_size);

        for i in 0..resolved_size {
            let engine = Self::create_slot_engine(&bundle_source, 0)?;
            slots.push(SsrSlot {
                engine: Mutex::new(Some(engine)),
            });
            available.push_back(i);
        }

        Ok(Self {
            bundle_path: path_buf,
            bundle_source: RwLock::new(bundle_source),
            generation: AtomicU64::new(0),
            slots,
            available: Mutex::new(available),
            condvar: Condvar::new(),
            acquire_timeout: Duration::from_millis(500),
            cache: Mutex::new(BoundedRenderCache::new(256, Duration::from_secs(60))),
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
    pub fn render(&self, component: &str, page_json: &str) -> Result<SSRResult> {
        let current_gen = self.generation.load(Ordering::Acquire);
        let cache_key = self.cache_key(component, page_json);

        if let Ok(mut cache_guard) = self.cache.lock()
            && let Some(res) = cache_guard.get(cache_key, current_gen)
        {
            return Ok(res);
        }

        let slot_idx = {
            let mut guard = self
                .available
                .lock()
                .map_err(|_| BundlerError::Poisoned("ssr available"))?;
            let start = Instant::now();
            while guard.is_empty() {
                let elapsed = start.elapsed();
                if elapsed >= self.acquire_timeout {
                    return Err(BundlerError::PoolExhausted);
                }
                let remaining = self.acquire_timeout.saturating_sub(elapsed);
                let (next_guard, timeout_res) = self
                    .condvar
                    .wait_timeout(guard, remaining)
                    .map_err(|_| BundlerError::Poisoned("ssr condvar"))?;
                guard = next_guard;
                if timeout_res.timed_out() && guard.is_empty() {
                    return Err(BundlerError::PoolExhausted);
                }
            }
            guard.pop_front().ok_or(BundlerError::PoolExhausted)?
        };

        let lease = SlotLease {
            pool: self,
            idx: slot_idx,
        };

        let mut slot_guard = self.slots[lease.idx]
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

        let render_result = eng.ctx.with(|ctx| {
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

            Ok::<_, BundlerError>(SSRResult { head, body })
        })?;

        drop(slot_guard);
        drop(lease);

        if let Ok(mut cache_guard) = self.cache.lock() {
            cache_guard.insert(cache_key, render_result.clone(), current_gen);
        }

        Ok(render_result)
    }

    /// Invalidate the SSR cache and mark all slot contexts as stale.
    pub fn invalidate(&self) {
        if let Ok(new_source) = std::fs::read_to_string(&self.bundle_path)
            && let Ok(mut src_guard) = self.bundle_source.write()
        {
            *src_guard = Arc::from(new_source);
        }

        self.generation.fetch_add(1, Ordering::Release);

        if let Ok(mut cache_guard) = self.cache.lock() {
            cache_guard.clear();
        }
    }

    /// Release all QuickJS runtimes and contexts.
    pub fn shutdown(&self) {
        for slot in &self.slots {
            if let Ok(mut guard) = slot.engine.lock() {
                *guard = None;
            }
        }
        if let Ok(mut cache_guard) = self.cache.lock() {
            cache_guard.clear();
        }
    }

    /// Return the current generation counter.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Return the number of cached render entries.
    pub fn cache_len(&self) -> usize {
        self.cache.lock().map(|c| c.len()).unwrap_or(0)
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
impl SSRPool {
    #[new]
    #[pyo3(signature = (bundle_path, pool_size=4))]
    fn py_new(bundle_path: &str, pool_size: usize) -> PyResult<Self> {
        Ok(Self::new(bundle_path, pool_size)?)
    }

    #[pyo3(name = "render")]
    fn py_render(&self, py: Python<'_>, component: &str, page_json: &str) -> PyResult<SSRResult> {
        let result = py.detach(|| self.render(component, page_json))?;
        Ok(result)
    }

    #[pyo3(name = "invalidate")]
    fn py_invalidate(&self) {
        self.invalidate();
    }

    #[pyo3(name = "shutdown")]
    fn py_shutdown(&self) {
        self.shutdown();
    }

    #[getter]
    #[pyo3(name = "generation")]
    fn py_generation(&self) -> u64 {
        self.generation()
    }

    #[getter]
    #[pyo3(name = "cache_len")]
    fn py_cache_len(&self) -> usize {
        self.cache_len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const MOCK_BUNDLE: &str = r#"
        globalThis.__ssr_render = function(component, props) {
            return {
                head: ["<title>" + (props.title || "Default") + "</title>"],
                body: "<div id='app'><h1>" + component + "</h1><p>" + (props.msg || "") + "</p></div>"
            };
        };
    "#;

    fn create_bundle_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        file.write_all(content.as_bytes())
            .expect("Failed to write bundle");
        file.flush().expect("Failed to flush bundle");
        file
    }

    #[test]
    fn test_ssr_pool_creation() {
        let bundle = create_bundle_file(MOCK_BUNDLE);
        let pool = SSRPool::new(bundle.path().to_str().unwrap(), 2);
        assert!(pool.is_ok(), "Pool creation failed: {:?}", pool.err());
        let pool = pool.unwrap();
        assert_eq!(pool.generation(), 0);
        assert_eq!(pool.slots.len(), 2);
    }

    #[test]
    fn test_ssr_render_basic() {
        let bundle = create_bundle_file(MOCK_BUNDLE);
        let pool = SSRPool::new(bundle.path().to_str().unwrap(), 2).unwrap();

        let result = pool.render("Home", r#"{"title":"My Page","msg":"Welcome"}"#);
        assert!(result.is_ok(), "Render failed: {:?}", result.err());
        let result = result.unwrap();

        assert_eq!(result.head, vec!["<title>My Page</title>"]);
        assert!(result.body.contains("<h1>Home</h1>"));
        assert!(result.body.contains("<p>Welcome</p>"));
    }

    #[test]
    fn test_ssr_render_cached() {
        let bundle = create_bundle_file(MOCK_BUNDLE);
        let pool = SSRPool::new(bundle.path().to_str().unwrap(), 2).unwrap();

        let props = r#"{"title":"Cached","msg":"First"}"#;
        let res1 = pool.render("App", props).unwrap();
        let res2 = pool.render("App", props).unwrap();

        assert_eq!(res1.head, res2.head);
        assert_eq!(res1.body, res2.body);
        assert_eq!(pool.cache_len(), 1);
    }

    #[test]
    fn test_ssr_cache_key_different_for_different_input() {
        let bundle = create_bundle_file(MOCK_BUNDLE);
        let pool = SSRPool::new(bundle.path().to_str().unwrap(), 1).unwrap();

        let key1 = pool.cache_key("Home", r#"{"a":1}"#);
        let key2 = pool.cache_key("About", r#"{"a":1}"#);
        let key3 = pool.cache_key("Home", r#"{"a":2}"#);

        assert_ne!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key2, key3);
    }

    #[test]
    fn test_ssr_missing_render_function() {
        let bundle = create_bundle_file("const x = 42;");
        let pool = SSRPool::new(bundle.path().to_str().unwrap(), 1).unwrap();

        let result = pool.render("Home", "{}");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, BundlerError::Ssr(_)),
            "Expected Ssr error, got: {err:?}"
        );
    }

    #[test]
    fn test_ssr_pool_multiple_contexts() {
        let bundle = create_bundle_file(MOCK_BUNDLE);
        let pool = SSRPool::new(bundle.path().to_str().unwrap(), 4).unwrap();

        for i in 0..8 {
            let props = format!(r#"{{"title":"Page {i}","msg":"{i}"}}"#);
            let result = pool.render("Test", &props);
            assert!(result.is_ok(), "Render {i} failed: {:?}", result.err());
        }
    }

    #[test]
    fn test_ssr_invalidate_rebuilds_slot_from_updated_bundle() {
        let mut bundle = create_bundle_file(MOCK_BUNDLE);
        let pool = SSRPool::new(bundle.path().to_str().unwrap(), 1).unwrap();

        let res1 = pool.render("App", r#"{"title":"v1"}"#).unwrap();
        assert_eq!(res1.head, vec!["<title>v1</title>"]);

        const UPDATED_BUNDLE: &str = r#"
            globalThis.__ssr_render = function(component, props) {
                return {
                    head: ["<title>UPDATED: " + props.title + "</title>"],
                    body: "<div>updated</div>"
                };
            };
        "#;
        bundle.write_all(UPDATED_BUNDLE.as_bytes()).unwrap();
        bundle.flush().unwrap();

        pool.invalidate();
        assert_eq!(pool.generation(), 1);
        assert_eq!(pool.cache_len(), 0);

        let res2 = pool.render("App", r#"{"title":"v2"}"#).unwrap();
        assert_eq!(res2.head, vec!["<title>UPDATED: v2</title>"]);
    }

    #[test]
    fn test_ssr_shutdown_clears_state() {
        let bundle = create_bundle_file(MOCK_BUNDLE);
        let pool = SSRPool::new(bundle.path().to_str().unwrap(), 2).unwrap();

        let _ = pool.render("App", r#"{"title":"test"}"#);
        assert_eq!(pool.cache_len(), 1);

        pool.shutdown();
        assert_eq!(pool.cache_len(), 0);
        for slot in &pool.slots {
            assert!(slot.engine.lock().unwrap().is_none());
        }
    }

    #[test]
    fn test_ssr_pool_concurrent_checkout_waits_for_slot() {
        let bundle = create_bundle_file(MOCK_BUNDLE);
        let pool = Arc::new(SSRPool::new(bundle.path().to_str().unwrap(), 2).unwrap());

        let mut handles = Vec::new();
        for i in 0..8 {
            let pool_clone = pool.clone();
            handles.push(std::thread::spawn(move || {
                let props = format!(r#"{{"title":"Concurrent {i}","msg":"{i}"}}"#);
                pool_clone.render("App", &props)
            }));
        }

        for h in handles {
            let res = h.join().expect("Thread panicked");
            assert!(res.is_ok(), "Concurrent render failed: {:?}", res.err());
        }
    }

    #[test]
    fn test_ssr_pool_cache_bounded_eviction() {
        let bundle = create_bundle_file(MOCK_BUNDLE);
        let pool = SSRPool::new(bundle.path().to_str().unwrap(), 2).unwrap();

        for i in 0..300 {
            let props = format!(r#"{{"title":"Page {i}","id":{i}}}"#);
            let _ = pool.render("App", &props).unwrap();
        }

        assert!(
            pool.cache_len() <= 256,
            "Cache length {} exceeded bound of 256",
            pool.cache_len()
        );
    }
}
