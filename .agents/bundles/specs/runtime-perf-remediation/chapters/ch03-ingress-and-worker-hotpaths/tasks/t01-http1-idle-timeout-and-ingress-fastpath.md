---
type: Task
id: "runtime-perf-remediation:ch03:t01"
chapter_id: ch03-ingress-and-worker-hotpaths
title: Lazy IdleTimeoutStream, connection-pinned shards, and single-pass headers
description: Replace per-I/O Sleep resets with an activity flag, pin collector shards per accepted connection, gate empty-body header scans, and encode HeaderMap in one pass.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md
  - src/rs/vertebra-http/src/server.rs
  - src/rs/vertebra-http/src/response.rs
files:
  - src/rs/vertebra-http/src/server.rs
  - src/rs/vertebra-http/src/response.rs
tests:
  - src/rs/vertebra-http/src/server.rs
  - src/rs/vertebra-http/src/response.rs
estimated_loc: 140
plan_revision: 2
plan_commit: "86527cf1"
commit: "6e72abac"
sources:
  - id: src-http-server
    resource: src/rs/vertebra-http/src/server.rs::IdleTimeoutStream
    title: IdleTimeoutStream and HTTP/1.1 + TLS connection loops
  - id: src-http-response
    resource: src/rs/vertebra-http/src/response.rs::dispatch_request_inner
    title: Request body gate and compact header encoding
tags:
  - http-ingress
  - http1
  - performance
---

# Lazy IdleTimeoutStream, Connection-Pinned Shards, and Single-Pass Headers

## Objective & Scope Boundary

Eliminate per-request Tokio timer-wheel resets, global `next_shard` atomic increments, unnecessary `CONTENT_LENGTH`/`CONTENT_ENCODING` header scans on empty requests, and double `HeaderMap` iteration in `vertebra-http` [^src-http-server] [^src-http-response].

Out of scope: `BackpressureController`, `RequestBatcher`, and `ShmDispatcher` internals (covered by `t02`–`t04`).

## Bounded Read Manifest

- Chapter spec: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch03-ingress-and-worker-hotpaths/spec.md`
- Source files: `src/rs/vertebra-http/src/server.rs`, `src/rs/vertebra-http/src/response.rs`

## Contracts & Edge Cases

Follow the contracts for `ServerState::{next_collector_shard, collector_for_shard}` and `handle_with_peer_shard` defined in `spec.md` Section 2.2:

- **`IdleTimeoutStream<S>`**: Add `activity: bool` initialized to `false`. Set `self.activity = true` on non-empty `poll_read` and `poll_write` instead of calling `Sleep::reset`. When `poll_read` sees `Poll::Pending` and `sleep.as_mut().poll(cx)` returns `Poll::Ready(())`:
  - If `self.activity` is `true`, clear `self.activity = false`, call `sleep.as_mut().reset(tokio::time::Instant::now() + self.timeout)`, and poll `sleep.as_mut().poll(cx)` once to register the new waker; return `TimedOut` only if that immediate re-poll is also `Ready`.
  - If `self.activity` is `false`, return `Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, "HTTP/1 connection idle timeout")))`.
  - When `timeout.is_zero()`, `deadline` stays `None`.
- **Connection shard affinity**: Call `let shard_idx = state.next_collector_shard();` once in `serve_plain` (both HTTP/1 fastpath and h2c/auto branches) and `serve_tls`, capturing `shard_idx: usize` in `service_fn` and calling `handle_with_peer_shard(req, peer, shard_idx, state)`. Keep `handle`, `handle_with_peer`, and `handle_buffered` compatible by passing `collector_shard: Option<usize>` (`None` falls back to `state.get_collector()`).
- **Body header scan gate**: Inside `dispatch_request_inner`, move `CONTENT_LENGTH` and `CONTENT_ENCODING` lookups out of the top-level path so `RequestBodySource::Incoming(incoming)` with `incoming.size_hint().exact() == Some(0)` returns `Ok((Bytes::new(), false))` without inspecting either header. Borrow `active_encoding: Option<&str>` inside the non-empty `Incoming` and `Buffered` arms to avoid `str::to_owned` heap allocation.
- **Single-pass `encode_header_map_compact`**: Return `Bytes::new()` when `headers.is_empty()`. Otherwise allocate `Vec::with_capacity(headers.len().saturating_mul(32))` and encode all `(name, value)` entries in a single `headers.iter()` loop, preserving `[nlen: u8][name][vlen: u16 LE][value]` wire layout, duplicate headers, and length clamping.

## Test Matrix

| Case | Verification |
| :--- | :--- |
| Active `IdleTimeoutStream` survives past first `timeout` | Mock stream with reads/writes before timer fire resets lazily once and succeeds without `TimedOut` under `tokio::test(start_paused = true)`. |
| Idle `IdleTimeoutStream` expires | Mock stream with zero activity returns `io::ErrorKind::TimedOut` when `timeout` elapses. |
| Zero timeout disables timer | `IdleTimeoutStream::new(inner, Duration::ZERO)` never times out. |
| Connection shard affinity | `ServerState::next_collector_shard` distributes connections round-robin across shards; `handle_with_peer_shard` delivers repeated requests on one connection to the pinned shard without touching other shards. |
| Single-pass `encode_header_map_compact` | Empty map, standard headers, duplicate header names, and values exceeding 32-byte heuristic decode identically via `iter_raw_headers`. |
| Empty-body `Incoming` vs non-empty/decompressed body | Empty `Incoming` skips body collection cleanly; gzip-encoded non-empty body still decompresses and rewrites `CONTENT_LENGTH`/`CONTENT_ENCODING`. |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http -- server::tests response::tests
```

## Acceptance Criteria

- `IdleTimeoutStream` performs zero `Sleep::reset` calls on active reads/writes until the timer fires.
- Accepted plain and TLS connections pin one collector shard per connection and never increment `next_shard` per request.
- Empty `Incoming` requests perform zero `CONTENT_LENGTH`/`CONTENT_ENCODING` lookups and `encode_header_map_compact` iterates `HeaderMap` exactly once.
- All `vertebra-http` `server` and `response` unit tests pass.

[^src-http-server]: `src/rs/vertebra-http/src/server.rs::IdleTimeoutStream` — IdleTimeoutStream and HTTP/1.1 + TLS connection loops.
[^src-http-response]: `src/rs/vertebra-http/src/response.rs::dispatch_request_inner` — Request body gate and compact header encoding.
