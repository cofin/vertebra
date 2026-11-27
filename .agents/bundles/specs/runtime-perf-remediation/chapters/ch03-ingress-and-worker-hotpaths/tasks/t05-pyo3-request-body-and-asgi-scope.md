---
type: Task
id: "runtime-perf-remediation:ch03:t05"
chapter_id: ch03-ingress-and-worker-hotpaths
title: Direct Request::body and shared-root bitmask-drop AsgiScope
description: Construct PyBytes directly from BatchSnapshot in Request::body and optimize AsgiScope with Arc<str> root_path and populated_mask-guided MaybeUninit drop.
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md
  - src/rs/vertebra-py/src/request.rs
  - src/rs/vertebra-py/src/asgi.rs
files:
  - src/rs/vertebra-py/src/request.rs
  - src/rs/vertebra-py/src/asgi.rs
tests:
  - src/rs/vertebra-py/src/asgi.rs
  - src/py/tests/unit/server/test_hotpath_dispatch_and_freethreaded.py
estimated_loc: 160
plan_revision: 2
plan_commit: "86527cf1"
commit: "6e72abac"
sources:
  - id: src-py-request
    resource: src/rs/vertebra-py/src/request.rs::Request
    title: Request proxy and body getter
  - id: src-py-asgi
    resource: src/rs/vertebra-py/src/asgi.rs::AsgiScope
    title: Lazy 24-slot ASGI 3.0 scope and inline batch driver
tags:
  - pyo3
  - asgi
  - performance
---

# Direct Request::body and Shared-Root Bitmask-Drop AsgiScope

## Objective & Scope Boundary

Eliminate the intermediate `OnceLock<Bytes>` heap copy in `Request::body`, share batch-level `Arc<str>` for `AsgiScope::root_path`, and replace 24-slot `Option` initialization and drop scanning with a `populated_mask: u32` bitmask over `[MaybeUninit<Py<PyAny>>; 24]` [^src-py-request] [^src-py-asgi].

Out of scope: RSGI protocol/scope (`t06`) and Python worker dispatch (`t07`).

## Bounded Read Manifest

- Chapter spec: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md`
- Source files: `src/rs/vertebra-py/src/request.rs`, `src/rs/vertebra-py/src/asgi.rs`

## Contracts & Edge Cases

Follow `AsgiScope::new_with_shared_root` defined in `spec.md` Section 2.2:

- **`Request::body` (`src/rs/vertebra-py/src/request.rs`)**: Read `let slice = self.batch.body(self.index);`. Return `None` when `slice.is_empty()` and `Some(PyBytes::new(py, slice))` otherwise, bypassing `self.get_body_bytes()` so `cached_body: OnceLock<Bytes>` stays untouched on the Python getter path. Keep `pub fn get_body_bytes(&self) -> &Bytes` intact for any Rust callers.
- **Shared `root_path: Arc<str>` (`src/rs/vertebra-py/src/asgi.rs`)**: Store `root_path: Arc<str>` on `AsgiScope`. Add `AsgiScope::new_with_shared_root(batch, index, root_path: Arc<str>)` and keep `AsgiScope::new(batch, index, root_path: String)` delegating via `Arc::from(root_path)`. In `drive_asgi_targets_inline`, allocate `let shared_root: Arc<str> = Arc::from(normalized_root);` once per batch and pass `Arc::clone(&shared_root)` per request.
- **Bitmask-tracked `cached` slots (`src/rs/vertebra-py/src/asgi.rs`)**:
  - Replace `cached: [Option<Py<PyAny>>; KNOWN_KEYS.len()]` with `cached: [std::mem::MaybeUninit<Py<PyAny>>; KNOWN_KEYS.len()]` and `populated_mask: u32` (initialized to `0` and `[const { MaybeUninit::uninit() }; KNOWN_KEYS.len()]`).
  - Route all slot reads, writes, and clears through three `#[inline]` helpers: `get_cached(&self, idx) -> Option<&Py<PyAny>>`, `set_cached(&mut self, idx, val)`, and `clear_cached(&mut self, idx)`.
  - Soundness invariant: clear the bit `self.populated_mask &= !(1u32 << idx)` BEFORE calling `unsafe { self.cached[idx].assume_init_drop() }` in `clear_cached` and before overwriting in `set_cached`, and clear `self.populated_mask = 0` at the top of `impl Drop for AsgiScope` before iterating set bits via `mask.trailing_zeros()` and `mask &= mask - 1`. Document every `unsafe` block with a `// SAFETY:` comment.

## Test Matrix

| Case | Verification |
| :--- | :--- |
| `Request.body` empty vs non-empty | Empty body returns `None`; non-empty body returns exact `bytes` matching `BatchSnapshot` on repeated accesses without populating `cached_body`. |
| `AsgiScope` get/set/overwrite/delete/setdefault/pop lifecycle | Populating known keys, overwriting existing slots, deleting slots, calling `setdefault` on `state`/`extensions`, and dropping scopes leak zero references and maintain exact mapping semantics. |
| `AsgiScope` shared `root_path` stripping | `drive_asgi_batch` with non-empty `root_path="/api"` strips `path` and `raw_path` identically across all requests in a multi-item batch. |
| Free-threaded & GIL dispatch tests | `test_hotpath_dispatch_and_freethreaded.py` passes with rebuilt `_vertebra`. |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py
make dev-ext EXT=core
uv run --no-sync pytest src/py/tests/unit/server/test_hotpath_dispatch_and_freethreaded.py
```

## Acceptance Criteria

- `Request::body` creates `PyBytes` directly from `self.batch.body(self.index)` with zero `OnceLock<Bytes>` intermediate copy.
- `AsgiScope` allocates `Arc<str>` once per batch in `drive_asgi_targets_inline` and drops only `populated_mask` set bits via `trailing_zeros()`.
- Every `unsafe` block in `AsgiScope` carries a clear `// SAFETY:` invariant comment and passes `make lint-rs` (`clippy`).
- All `vertebra-py` Rust tests and targeted Python hotpath tests pass.

[^src-py-request]: `src/rs/vertebra-py/src/request.rs::Request` — Request proxy and body getter.
[^src-py-asgi]: `src/rs/vertebra-py/src/asgi.rs::AsgiScope` — Lazy 24-slot ASGI 3.0 scope and inline batch driver.
