---
type: Task
id: "runtime-perf-remediation:ch03:t06"
chapter_id: ch03-ingress-and-worker-hotpaths
title: Lazy RsgiHttpProtocol streaming state and batch-shared RsgiServerMeta
description: Remove eager Arc allocations on unary RSGI responses by making streaming state lazy and share batch-invariant RsgiScope metadata via Arc<RsgiServerMeta>.
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md
  - src/rs/vertebra-py/src/rsgi/http.rs
  - src/rs/vertebra-py/src/rsgi/scope.rs
files:
  - src/rs/vertebra-py/src/rsgi/http.rs
  - src/rs/vertebra-py/src/rsgi/scope.rs
tests:
  - src/rs/vertebra-py/src/rsgi/http.rs
  - src/py/tests/unit/server/test_hotpath_dispatch_and_freethreaded.py
estimated_loc: 220
plan_revision: 2
plan_commit: "86527cf1"
commit: null
sources:
  - id: src-py-rsgi-http
    resource: src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol
    title: RSGI v1.6 HTTP protocol handler and batch driver
  - id: src-py-rsgi-scope
    resource: src/rs/vertebra-py/src/rsgi/scope.rs::RsgiScope
    title: Lazy RSGI v1.6 scope
tags:
  - pyo3
  - rsgi
  - performance
---

# Lazy RsgiHttpProtocol Streaming State and Batch-Shared RsgiServerMeta

## Objective & Scope Boundary

Eliminate both eager `Arc` allocations per unary RSGI request in `RsgiHttpProtocol::from_batch` and per-request metadata string allocations in `RsgiScope::from_batch` [^src-py-rsgi-http] [^src-py-rsgi-scope].

Out of scope: `Request::body` and `AsgiScope` (`t05`) and Python worker orchestration (`t07`).

## Bounded Read Manifest

- Chapter spec: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md`
- Source files: `src/rs/vertebra-py/src/rsgi/http.rs`, `src/rs/vertebra-py/src/rsgi/scope.rs`

## Contracts & Edge Cases

Follow `RsgiServerMeta` and `RsgiScope::from_batch_with_meta` defined in `spec.md` Section 2.2:

- **Shared `Arc<RsgiServerMeta>` (`src/rs/vertebra-py/src/rsgi/scope.rs`)**:
  - Move `proto_kind`, `scheme_kind`, `root_path: Box<str>`, `server_override: Option<Box<str>>`, and `client_override: Option<Box<str>>` into `pub(crate) struct RsgiServerMeta` constructed via `RsgiServerMeta::new(proto, scheme, server, client, root_path) -> Arc<Self>`.
  - Store a single `meta: Arc<RsgiServerMeta>` on `RsgiScope`. Keep `RsgiScope::from_batch(...)` constructing `RsgiServerMeta::new(...)` and delegating to `RsgiScope::from_batch_with_meta` so `Request::rsgi_scope` and `RsgiScope::py_new` remain unchanged.
  - Preserve per-snapshot TLS detection inside `RsgiScope::scheme`: when `matches!(self.meta.scheme_kind, SchemeKind::Http)` and `(self.batch.flags() & 1) != 0`, return interned `"https"`.
  - In `drive_rsgi_targets_inline` (`src/rs/vertebra-py/src/rsgi/http.rs`), allocate `let shared_meta = RsgiServerMeta::new("http", scheme, server, client, normalized_root);` once per batch and pass `Arc::clone(&shared_meta)` per request.
- **Zero-Arc unary `RsgiHttpProtocol` & lazy streaming state (`src/rs/vertebra-py/src/rsgi/http.rs`)**:
  - Store `request_id: u64`, `direct_submit: bool`, `state: Mutex<HttpProtocolState>`, `disconnect_future: Mutex<Option<Py<PyAny>>>`, and `disconnected: AtomicBool` inline on `RsgiHttpProtocol` (zero outer `Arc` allocation in `from_batch`, `new`, and `py_new`).
  - Add `stream: Option<Arc<RsgiStreamState>>` inside `HttpProtocolState` (initially `None`). Allocate `Arc<RsgiStreamState>` ONLY inside `response_stream()` (and `RsgiTransport::new(chunks)`). Inside `RsgiStreamState`, store `stream_chunks: Option<Arc<Mutex<Vec<u8>>>>` initialized only when `!direct_submit` (or when constructed via `RsgiTransport::new(chunks)`).
  - Disconnect propagation: when `response_stream()` creates `RsgiStreamState`, seed its `disconnected` atomic from `self.disconnected.load(Ordering::Acquire)` and move any pending `disconnect_future` (or share disconnect signaling) so `notify_disconnect()` before or after `response_stream()` and `PyConnectionResetError` inside `RsgiTransport::send_chunk()` both mark `stream_closed = true` and resolve `client_disconnect()`.
  - Preserve exact unary, file, file-range, streaming finalization, and error suppression semantics across `take_completed_for_batch`, `take_error_for_batch`, `finalize_async_return`, `finalize_async_error`, and `take_response`.

## Test Matrix

| Case | Verification |
| :--- | :--- |
| Existing disconnect-before-first-send test | `disconnect_before_first_send_suppresses_spurious_500_on_error_finalization` passes unchanged. |
| Unary RSGI responses (`response_empty`, `response_str`, `response_bytes`, `response_file`, `response_file_range`) | `st.stream` stays `None` through completion and `take_response` / batch flush returns exact status, headers, and body. |
| Streaming RSGI (direct submit & buffered `take_response`) | `response_stream()` lazily allocates `RsgiStreamState`, buffers chunks when `!direct_submit`, submits progressive frames when `direct_submit`, and resolves `client_disconnect()` on peer reset. |
| Batch-shared `RsgiServerMeta` with custom server/client/root_path | Multi-item `drive_rsgi_batch` preserves stripped `path`, `root_path`, custom `server`/`client`, and TLS flag `scheme == "https"`. |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py rsgi
make dev-ext EXT=core
uv run --no-sync pytest src/py/tests/unit/server/test_hotpath_dispatch_and_freethreaded.py
```

## Acceptance Criteria

- Unary `RsgiHttpProtocol::from_batch` performs zero `Arc` allocations (`stream` remains `None` unless `response_stream()` is invoked).
- `stream_chunks` is `Option<Arc<Mutex<Vec<u8>>>>` allocated only for buffered streams.
- `drive_rsgi_targets_inline` allocates one `Arc<RsgiServerMeta>` per batch regardless of batch size.
- All `vertebra-py` RSGI Rust tests and Python hotpath tests pass.

[^src-py-rsgi-http]: `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol` — RSGI v1.6 HTTP protocol handler and batch driver.
[^src-py-rsgi-scope]: `src/rs/vertebra-py/src/rsgi/scope.rs::RsgiScope` — Lazy RSGI v1.6 scope.
