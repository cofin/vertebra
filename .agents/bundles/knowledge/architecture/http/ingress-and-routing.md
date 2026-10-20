---
type: Guide
title: "HTTP/1.1 & HTTP/2 Ingress & Native Routing"
description: "Tokio HTTP/1.1 and HTTP/2 Server listener loop, TLS, matchit Router with FNV-1a HandlerId, RouteTable, and static asset serving."
status: stable
tags: [architecture, http, routing, tls, rust]
sources:
  - resource: src/rs/vertebra-http/src/server.rs
  - resource: src/rs/vertebra-http/src/router.rs
  - resource: src/rs/vertebra-http/src/route_table.rs
  - resource: src/rs/vertebra-http/src/tls.rs
  - resource: src/rs/vertebra-http/src/assets.rs
  - resource: src/rs/vertebra-core/src/router.rs
---

# HTTP/1.1 & HTTP/2 Ingress & Native Routing

[`vertebra-http`](../../../../../src/rs/vertebra-http/src/lib.rs) terminates TCP/TLS connections, matches routes in Rust before touching Python, serves static assets directly from Tokio, and feeds dynamic requests into the batching pipeline.

## 1. Tokio Listener & Connection Pipeline (`server.rs`)

[`Server`](../../../../../src/rs/vertebra-http/src/server.rs) runs a multi-threaded Tokio runtime configured by [`ServerConfig`](../../../../../src/rs/vertebra-http/src/server.rs):

- **Socket Tuning**: Binds with `SO_REUSEADDR`, sets `TCP_NODELAY` on accepted sockets, and limits concurrent connections via an `Arc<Semaphore>` (`max_connections`, default `10,000`).
- **HTTP/1.1 Pipelining (`http1::Builder`)**: Enables `pipeline_flush(true)` and `keep_alive(true)` so pipelined HTTP/1.1 batches flush in a single `writev` syscall.
- **HTTP/2 & TLS (`tls.rs`)**: When [`TlsConfig`](../../../../../src/rs/vertebra-http/src/tls.rs) is supplied (or `TlsAcceptor::self_signed` is used in `vertebra dev`), `tokio-rustls` negotiates ALPN `h2` and `http/1.1`, routing `h2` streams through `hyper::server::conn::http2::Builder` (`TokioExecutor`) with configurable stream/connection windows and max concurrent streams.
- **Graceful Drain**: On `SIGINT` / `SIGTERM` or `ShutdownHandle::shutdown()`, the listener stops accepting new connections and waits up to `drain_timeout` (default `30s`) for in-flight requests to complete.

## 2. Native Rust Routing (`Router` & `RouteTable`)

Route resolution occurs in Rust before any IPC or Python FFI work:

1. **Per-Method Radix Trees (`vertebra-http/src/router.rs`)**: [`Router`](../../../../../src/rs/vertebra-http/src/router.rs) maintains per-method `matchit::Router<RouteTarget>` trees (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS`) plus an `all_methods` fallback map to return `405 Method Not Allowed` with an accurate `Allow` header when a path exists under another verb.
2. **Typed Path Normalization**: [`normalize_typed_path`](../../../../../src/rs/vertebra-http/src/router.rs) (also in [`vertebra-core/src/router.rs`](../../../../../src/rs/vertebra-core/src/router.rs)) rewrites Python/Litestar-style `{id:int}`, Express-style `:id`, and catch-all `*rest` / `{path:path}` segments into `matchit 0.9` `{id}` and `{*rest}` syntax.
3. **Deterministic FNV-1a `HandlerId`**: `compute_handler_id(method, path)` hashes `"METHOD:path"` with 64-bit FNV-1a (folded to 32-bit `HandlerId`) so Rust and Python compute identical handler IDs without cross-language registration round-trips.
4. **Lock-Free Hot Reload (`RouteTable`)**: [`RouteTable`](../../../../../src/rs/vertebra-http/src/route_table.rs) wraps `ArcSwap<Router>` so `vertebra dev` can atomically swap the entire routing table (`RouteTable::swap`) while in-flight requests read a wait-free `Guard<Arc<Router>>`.

## 3. Zero-Python Static Asset Serving (`assets.rs`)

When a route matches `RouteTarget::StaticDir` (or the static prefix filter matches before dynamic routing), [`StaticAssetServer`](../../../../../src/rs/vertebra-http/src/assets.rs) serves the file directly from Rust with path-traversal canonicalization checks, MIME detection (`mime_guess`), `ETag` / `If-None-Match` `304 Not Modified` handling, and optional `Cache-Control: public, max-age=31536000, immutable` headers for hashed bundle assets.
