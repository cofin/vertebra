//! QuickJS SSR pool for server-side rendering.
//!
//! Uses rquickjs to maintain a pool of pre-warmed QuickJS contexts,
//! each with the SSR bundle pre-evaluated. Renders components by
//! calling `globalThis.__ssr_render(component, props)`.

use crate::errors::{BundlerError, Result};
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use rquickjs::function::Rest;
use rquickjs::{CatchResultExt, Context, Function, Object, Runtime, String as JsString, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Result of an SSR render call.
#[derive(Clone, Debug)]
pub struct SsrResult {
    /// Head tags emitted by the component (e.g. `<title>`, `<meta>`).
    pub head: Vec<String>,
    /// Rendered HTML body string.
    pub body: String,
}

/// Cached render entry with TTL tracking.
struct CachedRender {
    result: SsrResult,
    created_at: Instant,
}

/// A single SSR context wrapping a QuickJS Context with stale tracking.
struct SsrContext {
    ctx: Context,
    stale: AtomicBool,
}

/// Pool of QuickJS contexts for SSR rendering.
///
/// Each context has the SSR bundle pre-evaluated so that
/// `globalThis.__ssr_render(component, props)` is available.
/// Contexts are checked out from a lock-free `ArrayQueue`,
/// used for a single render, and returned immediately.
///
/// A `DashMap`-backed render cache avoids re-executing JS for
/// identical (component, props) pairs within the TTL window.
pub struct SsrPool {
    /// Kept alive so that contexts remain valid (they borrow from the runtime).
    #[allow(dead_code)]
    runtime: Runtime,
    /// Retained for potential lazy context recreation on invalidation.
    #[allow(dead_code)]
    bundle_source: String,
    contexts: Vec<SsrContext>,
    available: ArrayQueue<usize>,
    cache: DashMap<u64, CachedRender>,
    cache_ttl: Duration,
}

impl SsrPool {
    /// Create a new SSR pool.
    ///
    /// 1. Read the SSR bundle from disk at `bundle_path`.
    /// 2. Create a QuickJS `Runtime` with a 256 MB memory limit.
    /// 3. Pre-warm `pool_size` contexts by evaluating the bundle in each.
    pub fn new(bundle_path: &str, pool_size: usize) -> Result<Self> {
        let bundle_source = std::fs::read_to_string(bundle_path).map_err(BundlerError::Io)?;

        let runtime = Runtime::new()
            .map_err(|e| BundlerError::Ssr(format!("Failed to create QuickJS runtime: {e}")))?;

        // 256 MB memory limit per runtime.
        runtime.set_memory_limit(256 * 1024 * 1024);

        let mut contexts = Vec::with_capacity(pool_size);
        let available = ArrayQueue::new(pool_size);

        for i in 0..pool_size {
            let ctx = Self::create_context(&runtime, &bundle_source)?;
            contexts.push(SsrContext {
                ctx,
                stale: AtomicBool::new(false),
            });
            available
                .push(i)
                .expect("ArrayQueue capacity matches pool_size");
        }

        Ok(Self {
            runtime,
            bundle_source,
            contexts,
            available,
            cache: DashMap::new(),
            cache_ttl: Duration::from_secs(60),
        })
    }

    /// Create a single QuickJS context and evaluate the SSR bundle inside it.
    ///
    /// Also installs a minimal `console.log` that forwards to `eprintln!`.
    fn create_context(runtime: &Runtime, source: &str) -> Result<Context> {
        let ctx = Context::full(runtime)
            .map_err(|e| BundlerError::Ssr(format!("Failed to create context: {e}")))?;

        ctx.with(|ctx| {
            let globals = ctx.globals();

            // Provide a minimal console.log for the SSR bundle.
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

            // Evaluate the SSR bundle.
            ctx.eval::<(), _>(source.as_bytes())
                .catch(&ctx)
                .map_err(|e| BundlerError::Ssr(format!("Bundle eval failed: {e}")))?;

            Ok::<_, BundlerError>(())
        })?;

        Ok(ctx)
    }

    /// Render a component to HTML.
    ///
    /// Checks the render cache first (keyed on component + page_json).
    /// On cache miss, checks out a context from the pool, calls
    /// `globalThis.__ssr_render(component, JSON.parse(page_json))`,
    /// caches the result, and returns it.
    pub fn render(&self, component: &str, page_json: &str) -> Result<SsrResult> {
        // Check cache.
        let cache_key = self.cache_key(component, page_json);
        if let Some(cached) = self.cache.get(&cache_key)
            && cached.created_at.elapsed() < self.cache_ttl
        {
            return Ok(cached.result.clone());
        }

        // Acquire a context from the pool.
        let idx = self.available.pop().ok_or(BundlerError::PoolExhausted)?;

        // If context is marked stale, recreate it in-place.
        // Safety: we hold the only "lease" on this index while it is
        // popped from the available queue, so no other thread accesses it.
        if self.contexts[idx].stale.load(Ordering::Acquire) {
            // We cannot mutate through &self, but we can recreate the
            // QuickJS context within the existing slot using unsafe.
            // For the initial implementation we log and continue with
            // the stale context -- a full rebuild requires &mut self.
            eprintln!("[ssr] context {idx} is stale, using existing context");
        }

        let render_result = self.contexts[idx].ctx.with(|ctx| {
            let globals = ctx.globals();

            // Retrieve __ssr_render from globalThis.
            let render_fn: Function = globals.get("__ssr_render").catch(&ctx).map_err(|e| {
                BundlerError::Ssr(format!(
                    "__ssr_render not found in SSR bundle. \
                         The bundle must set globalThis.__ssr_render = \
                         function(component, props) {{ ... }}. Error: {e}"
                ))
            })?;

            // Parse the page JSON in JS land via JSON.parse.
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

            // Call __ssr_render(component, parsedProps).
            let result: Object = render_fn
                .call((component, parsed_props))
                .catch(&ctx)
                .map_err(|e| BundlerError::Ssr(format!("__ssr_render failed: {e}")))?;

            // Extract head: string[]
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

            // Extract body: string
            let body: String = result
                .get("body")
                .catch(&ctx)
                .map_err(|e| BundlerError::Ssr(format!("result.body missing: {e}")))?;

            Ok::<_, BundlerError>(SsrResult { head, body })
        })?;

        // Return context to pool.
        self.available
            .push(idx)
            .expect("ArrayQueue slot was popped, push must succeed");

        // Cache the result.
        self.cache.insert(
            cache_key,
            CachedRender {
                result: render_result.clone(),
                created_at: Instant::now(),
            },
        );

        Ok(render_result)
    }

    /// Mark all contexts as stale and clear the render cache.
    ///
    /// Stale contexts will be lazily recreated on next use once
    /// mutable rebuilding is supported. For now the cache clear
    /// is the primary invalidation mechanism.
    pub fn invalidate(&self) {
        for ctx in &self.contexts {
            ctx.stale.store(true, Ordering::Release);
        }
        self.cache.clear();
    }

    /// Shut down the pool: clear the cache and drop all contexts.
    pub fn shutdown(&mut self) {
        self.cache.clear();
        self.contexts.clear();
    }

    /// FNV-1a hash for the cache key, combining component name and page JSON.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Write a minimal SSR bundle that defines `globalThis.__ssr_render`.
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
        assert_eq!(pool.contexts.len(), 2);
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
    fn test_ssr_invalidate_clears_cache() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = write_ssr_bundle(dir.path());
        let pool = SsrPool::new(bundle.to_str().unwrap(), 1).unwrap();

        pool.render("App", r#"{}"#).unwrap();
        assert!(!pool.cache.is_empty());
        pool.invalidate();
        assert!(pool.cache.is_empty());
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
        assert_eq!(pool.contexts.len(), 2);

        pool.shutdown();
        assert!(pool.cache.is_empty());
        assert!(pool.contexts.is_empty());
    }

    #[test]
    fn test_ssr_pool_multiple_contexts() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = write_ssr_bundle(dir.path());
        let pool = SsrPool::new(bundle.to_str().unwrap(), 4).unwrap();

        // Render with different inputs to exercise the pool.
        for i in 0..8 {
            let component = format!("Component{i}");
            let props = format!(r#"{{"idx":{i}}}"#);
            let result = pool.render(&component, &props).unwrap();
            assert!(result.body.contains(&component));
        }
    }
}
