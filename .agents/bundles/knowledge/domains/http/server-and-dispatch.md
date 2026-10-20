---
type: Guide
title: "Rust HTTP Ingress, Batching & Dispatch Domain"
description: "Canonical domain types for Vertebra's Rust HTTP/1.1 & HTTP/2 ingress server, dual-path VertebraBody, router, sharded batcher, dispatcher, and backpressure."
status: stable
tags: [domains, http, routing, batching, streaming, rust]
sources:
  - resource: src/rs/vertebra-http/src/lib.rs
  - resource: src/rs/vertebra-http/src/server.rs
  - resource: src/rs/vertebra-http/src/response.rs
  - resource: src/rs/vertebra-http/src/router.rs
  - resource: src/rs/vertebra-http/src/route_table.rs
  - resource: src/rs/vertebra-http/src/collector.rs
  - resource: src/rs/vertebra-http/src/policy.rs
  - resource: src/rs/vertebra-http/src/dispatcher/mod.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm.rs
  - resource: src/rs/vertebra-http/src/direct_worker.rs
  - resource: src/rs/vertebra-http/src/correlation.rs
  - resource: src/rs/vertebra-http/src/backpressure.rs
  - resource: src/rs/vertebra-http/src/ring_factory.rs
---

# Rust HTTP Ingress, Batching & Dispatch Domain

[`vertebra-http`](../../../../../src/rs/vertebra-http/src/lib.rs) owns the Tokio HTTP/1.1 and HTTP/2 ingress server, dual-path unary/streaming response bodies, native radix routing, sharded request batching, in-flight correlation tracking, and worker ring provisioning.

## 1. Server, Response Bodies, Routing & Static Assets (`server.rs`, `response.rs`, `router.rs`, `route_table.rs`, `assets.rs`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`Server`](../../../../../src/rs/vertebra-http/src/server.rs) / [`ServerConfig`](../../../../../src/rs/vertebra-http/src/config.rs) | `vertebra_http::server` | Tokio TCP/TLS accept loop, HTTP/1.1 `pipeline_flush` & HTTP/2 connection serving, embedded Python worker spawning, and graceful drain. |
| [`VertebraBody`](../../../../../src/rs/vertebra-http/src/response.rs) / [`StreamingBody`](../../../../../src/rs/vertebra-http/src/response.rs) | `vertebra_http::response` | Dual-path `Either<Full<Bytes>, StreamingBody>` (`full_body` and `stream_body`) supporting zero-allocation unary responses and multi-frame chunk/trailer streams. |
| [`Router`](../../../../../src/rs/vertebra-http/src/router.rs) / [`RouteMatch`](../../../../../src/rs/vertebra-http/src/router.rs) | `vertebra_http::router` | Per-method `matchit 0.9` radix router with `normalize_typed_path` (`{id:int}`, `:id`, `*rest` $\to$ `{id}`, `{*rest}`) and deterministic FNV-1a `HandlerId`. |
| [`RouteTable`](../../../../../src/rs/vertebra-http/src/route_table.rs) | `vertebra_http::route_table` | Concurrent route table supporting atomic route registration and hot swapping during `vtb dev`. |
| [`StaticAssets`](../../../../../src/rs/vertebra-http/src/assets.rs) | `vertebra_http::assets` | Zero-Python static file handler with canonical path traversal guards, ETag / `304 Not Modified`, and immutable cache headers. |
| [`TlsAcceptor`](../../../../../src/rs/vertebra-http/src/tls.rs) / [`TlsConfig`](../../../../../src/rs/vertebra-http/src/tls.rs) | `vertebra_http::tls` | `tokio-rustls` acceptor with PEM file loading, self-signed dev certificate generation (`rcgen 0.14`), and ALPN `h2` / `http/1.1` negotiation. |

## 2. Batching, Dispatch, Correlation & Ring Provisioning

| Symbol | Source | Responsibility |
|---|---|---|
| [`RequestBatcher`](../../../../../src/rs/vertebra-http/src/collector.rs) / [`PendingRequest`](../../../../../src/rs/vertebra-http/src/collector.rs) | `vertebra_http::collector` | 8-shard cache-line-aligned request accumulator with zero-timer idle sleep and adaptive high-load flushing. |
| [`BatchPolicy`](../../../../../src/rs/vertebra-http/src/policy.rs) | `vertebra_http::policy` | Configurable batch size (`max_batch_size`), linger duration, and adaptive immediate-drain policy. |
| [`Dispatcher`](../../../../../src/rs/vertebra-http/src/dispatcher/mod.rs) / [`ShmDispatcher`](../../../../../src/rs/vertebra-http/src/dispatcher/shm.rs) / [`DirectWorkerPool`](../../../../../src/rs/vertebra-http/src/direct_worker.rs) | `vertebra_http::dispatcher` | Routes requests across [`Direct`](../../../../../src/rs/vertebra-http/src/dispatcher/direct.rs), [`Bridge`](../../../../../src/rs/vertebra-http/src/dispatcher/bridge.rs), or [`SharedMemory`](../../../../../src/rs/vertebra-http/src/dispatcher/shm.rs) modes with power-of-two ring sharding and multi-chunk streaming. |
| [`PendingRequests`](../../../../../src/rs/vertebra-http/src/correlation.rs) (`InFlightRequests`) | `vertebra_http::correlation` | 256-shard (`65,536` total slots) correlation table pairing `correlation_id` to unary `oneshot::Sender` or streaming `mpsc::Sender` channels with deadline sweeps. |
| [`BackpressureController`](../../../../../src/rs/vertebra-http/src/backpressure.rs) / [`BackpressureConfig`](../../../../../src/rs/vertebra-http/src/backpressure.rs) | `vertebra_http::backpressure` | Concurrency semaphore (`max_in_flight`) and sliding-window circuit breaker (`Closed`, `Open`, `HalfOpen`). |
| [`RingFactory`](../../../../../src/rs/vertebra-http/src/ring_factory.rs) | `vertebra_http::ring_factory` | Allocates inheritable worker `RingPair`s and manages multi-process SHM worker descriptors. |
