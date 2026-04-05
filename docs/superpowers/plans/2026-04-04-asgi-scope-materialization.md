# ASGI Scope Materialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce Python worker overhead by materializing the ASGI scope dictionary directly in Rust/FFI, eliminating multiple property-access roundtrips and dictionary copies.

**Architecture:** Add a `asgi_scope` method to `VertebraRequest` in `vertebra-py` that returns a pre-populated Python dictionary following the ASGI 3.0 spec. Update the Python `AsgiAdapter` to use this method instead of manual building.

**Tech Stack:** Rust (PyO3), Python (asyncio/ASGI).

---

### Task 1: Add `asgi_scope` to `VertebraRequest` in Rust

**Files:**
- Modify: `src/rs/vertebra-py/src/request.rs`

- [ ] **Step 1: Implement `asgi_scope` method**

Add a method to `VertebraRequest` that creates a `PyDict` and populates it with standard ASGI keys.

```rust
    fn asgi_scope<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("type", "http")?;
        d.set_item("asgi", {
            let inner = PyDict::new(py);
            inner.set_item("version", "3.0")?;
            inner.set_item("spec_version", "2.3")?;
            inner
        })?;
        d.set_item("http_version", self.http_version(py))?;
        d.set_item("method", self.method(py).to_string().to_uppercase())?;
        d.set_item("path", self.path(py))?;
        d.set_item("raw_path", self.raw_path(py))?;
        d.set_item("query_string", self.raw_query(py))?;
        d.set_item("root_path", "")?; // Default for now
        d.set_item("scheme", "http")?; // Default for now
        d.set_item("headers", self.get_headers().asgi_items(py)?)?;
        d.set_item("state", PyDict::new(py))?;
        d.set_item("extensions", PyDict::new(py))?;
        d.set_item("server", py.None())?;
        d.set_item("client", py.None())?;
        Ok(d)
    }
```

- [ ] **Step 2: Build Rust project**

Run: `make rs-build`
Expected: Compilation succeeds.

---

### Task 2: Verify `asgi_scope` in Python Unit Test

**Files:**
- Modify: `src/py/tests/unit/server/test_asgi.py`

- [ ] **Step 1: Write verification test using FFI if possible**

Since we can't easily mock `BatchSnapshot`, we'll focus on making `build_scope` transparently switch to `asgi_scope`.

---

### Task 3: Update Python `build_scope` to use `asgi_scope`

**Files:**
- Modify: `src/py/vertebra/server/_asgi/_scope.py`

- [ ] **Step 1: Update `build_scope` implementation**

```python
def build_scope(request: Any, *, root_path: str = "") -> Any:
    """Build an ASGI 3.0 HTTP connection scope using FFI materialization if available."""
    if hasattr(request, "asgi_scope"):
        scope = request.asgi_scope()
        if root_path:
            scope["root_path"] = root_path
        return scope
    
    # Fallback to legacy builder for non-FFI requests (mocks, etc.)
    # ... (existing implementation below)
```

- [ ] **Step 2: Run ASGI tests**

Run: `uv run pytest src/py/tests/unit/server/test_asgi.py`
Expected: PASS

- [ ] **Step 3: Commit changes**

```bash
git add src/rs/vertebra-py/src/request.rs src/py/vertebra/server/_asgi/_scope.py
git commit -m "perf(asgi): materialize scope in FFI to reduce property access overhead"
```

---

### Task 4: Final Throughput Verification

- [ ] **Step 1: Run performance benchmark**

Run: `make bench-hello` (or equivalent)
Expected: Higher RPS than baseline 45k. Target 150k.
