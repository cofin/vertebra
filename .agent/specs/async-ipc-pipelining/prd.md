# Master PRD: Async IPC Pipelining

**Epic:** `vertebra-k4k`
**Status:** Planning
**Created:** 2026-01-27

## Context (North Star)

The current IPC bridge architecture serializes all requests through `Arc<Mutex<Worker>>`, blocking tokio worker threads with spin-wait loops. Benchmarks show ~21 RPS uniformly across all endpoints - this is the benchmark tool timing out, not real throughput. The architecture fundamentally cannot scale.

**Goal:** Achieve maximum throughput by enabling true async pipelining - multiple concurrent requests per worker with zero blocking in the async runtime.

## Current Architecture Problems

1. **Mutex Serialization**: `Arc<Mutex<Worker>>` in `dispatcher.rs:19` allows only 1 in-flight request per worker
2. **Blocking Spin-Wait**: `spin_wait_or_park_with` in `bridge.rs:96` blocks tokio threads
3. **No Correlation**: Protocol assumes strict request-response ordering, preventing pipelining
4. **Single Eventfd**: Bidirectional notifications on same fd creates ambiguity
5. **Sequential Python**: Workers process one request at a time

## Target Architecture

```
HTTP Request → Tokio Task → Send to Ring (non-blocking)
                              ↓
                     AsyncFd wakes on eventfd
                              ↓
                     Correlation Map resolves response
                              ↓
                     Response returned to caller
```

- **Lock-free dispatch** with least-pending routing
- **u64 sequence IDs** for request/response correlation
- **AsyncFd** on eventfd for true async wake
- **Concurrent Python** processing via asyncio

## Roadmap (Chapters)

### Chapter 1: `async-eventfd`
**Scope:** Replace blocking eventfd operations with tokio AsyncFd integration.

- Wrap eventfd in `tokio::io::unix::AsyncFd`
- Implement `async fn wait_readable()` for non-blocking wake
- Remove all `spin_wait_or_park` patterns
- Separate eventfds for req/resp directions

### Chapter 2: `correlation-protocol`
**Scope:** Add request/response correlation for pipelining.

- Add u64 sequence ID to `RequestHeader` protocol
- Implement per-worker `DashMap<u64, oneshot::Sender>` for pending requests
- Update Python worker to echo sequence ID in responses
- Wire correlation through bridge dispatch

### Chapter 3: `lock-free-dispatch`
**Scope:** Remove `Arc<Mutex<Worker>>` and implement least-pending dispatch.

- Replace mutex with atomic pending counters
- Implement least-pending worker selection
- Allow multiple concurrent sends to same worker
- Handle backpressure when ring buffer full

### Chapter 4: `python-concurrent`
**Scope:** Enable concurrent request processing in Python workers.

- Refactor worker main loop to use `asyncio.gather`
- Process multiple in-flight requests simultaneously
- Maintain response ordering via correlation IDs
- Handle cancellation and timeouts per-request

## Global Constraints

- **No backwards compatibility** - clean break from current protocol
- **Zero blocking in async context** - all waits must be async
- **Maximum performance over simplicity** - choose efficient over easy
- **Clean up outdated code** - remove all spin-loop, mutex patterns

## Success Metrics

- Benchmark shows linear scaling with worker count
- No uniform RPS ceiling (current ~21 RPS timeout artifact)
- p99 latency under load stays reasonable
- Zero tokio worker thread blocking
