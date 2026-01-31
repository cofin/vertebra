# Litecore Patterns & Learnings

## 1. PyO3 & GIL Management
*   **Best Practice:** Release GIL (`Python::allow_threads` or `py.detach`) for ANY I/O or long-running CPU task.
*   **Context:** `litecore` runs the HTTP server. It cannot block the Tokio reactor waiting for Python's GIL.
*   **Optimistic GIL:**
    *   Try non-blocking operations first (e.g. check ring buffer).
    *   If available, proceed without releasing GIL (avoid context switch).
    *   If blocked, release GIL and wait on `eventfd`/`futex`.

## 2. RSGI vs ASGI
*   **Learning:** ASGI `scope` creation (dict allocation) is a major bottleneck (>20% of CPU in "Hello World").
*   **Solution:** Use **RSGI** (Rust Server Gateway Interface). Pass Rust structs directly to Python via PyO3.
*   **Fallback:** Keep ASGI for compatibility, but optimize the `scope` builder by using interned strings.

## 3. Polyglot Bundling (Rolldown)
*   **Strategy:** Use `rolldown` crate in-process. Avoid spawning `node` or `npm`.
*   **Asset Serving:**
    *   Development: Proxy to `vite` (running on ephemeral port).
    *   Production: Serve from `EmbeddedFS` (compressed bytes in binary).
    *   **Reflinking:** Use reflink/hardlink logic if caching builds on disk.

## 4. Hyper & Tokio
*   **HTTP/2:** Enabled via `hyper` builder.
*   **Runtime:** Use `tokio::runtime::Builder::new_multi_thread()`.
*   **Affinity:** Consider pinning Python interpreters to specific threads if using sub-interpreters (experimental) or processes.

## 5. Middleware Integration
*   **StatePlugin:** Use the `StatePlugin` trait to inject native resources (like `vertebral` rings) into the request lifecycle.
*   **Zero Setup:** Plugins should initialize once at startup and be available in `req.extensions` immediately.
