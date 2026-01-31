# Performance Analysis: Vertebra vs Granian

## Executive Summary

Vertebra achieves **14,444 RPS** while Granian achieves **62,364 RPS** (4.3x faster) on identical hardware. This analysis examines the architectural differences and identifies the fundamental performance ceiling imposed by Vertebra's IPC-based design.

## Benchmark Results (Intel Xeon E-2276G, 12 cores, 64 concurrent connections)

| Server | RPS | Avg Latency | P50 | P99 |
|--------|-----|-------------|-----|-----|
| **Granian** | 62,364 | 1.02ms | 0.99ms | 1.47ms |
| **Vertebra** | 14,444 | 4.42ms | 4.07ms | 36.9ms |
| **Uvicorn (h11)** | ~11,106 | - | - | - |
| **Uvicorn (httptools)** | ~28,400 | - | - | - |

**Key observation**: Vertebra's P99 latency (36.9ms) is 25x higher than Granian's (1.47ms). While Vertebra beats Uvicorn's pure-Python H11 implementation, it currently trails the optimized `httptools` implementation by ~50%.

## Architecture Comparison

### Granian (62K RPS)
```
                    ┌─────────────────────────────────┐
HTTP Request ──────▶│  Rust HTTP Server (hyper)       │
                    │                                 │
                    │   ┌─────────────────────────┐   │
                    │   │ Python via PyO3         │   │
                    │   │ (same process, no IPC)  │   │
                    │   │                         │   │
                    │   │ • Direct function call  │   │
                    │   │ • Zero serialization    │   │
                    │   │ • Shared GIL state      │   │
                    │   └─────────────────────────┘   │
HTTP Response ◀─────│                                 │
                    └─────────────────────────────────┘
```

### Vertebra (14K RPS)
```
                    ┌─────────────────────────┐      ┌───────────────────────┐
HTTP Request ──────▶│  Rust HTTP Server       │      │  Python Worker        │
                    │  (hyper)                │      │  (separate process)   │
                    │                         │      │                       │
                    │  1. Parse HTTP          │──────│                       │
                    │  2. Serialize (msgpack) │  IPC │  4. Deserialize       │
                    │  3. Write to ring       │◀────▶│  5. Run ASGI app      │
                    │  6. Notify (eventfd)    │      │  6. Serialize response│
                    │                         │      │  7. Write to ring     │
                    │  7. Wait for response   │      │  8. Notify (eventfd)  │
                    │  8. Read from ring      │      │                       │
                    │  9. Parse response      │      │                       │
HTTP Response ◀─────│ 10. Build HTTP response │      │                       │
                    └─────────────────────────┘      └───────────────────────┘
```

## Per-Request Overhead Analysis

### Granian Overhead
| Step | Cost |
|------|------|
| HTTP parse | ~50μs |
| PyO3 call | ~0.2μs |
| Python handler | ~50μs |
| Response build | ~20μs |
| **Total** | **~120μs** |

### Vertebra Overhead
| Step | Cost | Notes |
|------|------|-------|
| HTTP parse | ~50μs | Same |
| MessagePack serialize (request) | ~10μs | 48 field structs |
| Ring buffer write | ~5μs | memcpy + atomic ops |
| eventfd notify (syscall) | ~1μs | Conditional |
| **Context switch** | ~10-20μs | User→kernel→user |
| MessagePack deserialize (request) | ~10μs | |
| Python handler | ~50μs | Same |
| MessagePack serialize (response) | ~10μs | |
| Ring buffer write | ~5μs | |
| eventfd notify (syscall) | ~1μs | |
| **Context switch back** | ~10-20μs | |
| Ring buffer read | ~5μs | |
| MessagePack deserialize (response) | ~10μs | |
| Response build | ~20μs | |
| **Total** | **~200-250μs** | |

**Overhead delta**: ~100-130μs per request from IPC alone.

## IPC Hot Path Analysis

### Rust Side (`bridge.rs:59-165`)
```rust
pub async fn dispatch(req, worker) {
    // 1. Collect body bytes (allocation)
    let body_bytes = body.collect().await?.to_bytes();

    // 2. UUID generation (48 bytes allocation)
    let uuid = uuid::Uuid::new_v4().to_string();

    // 3. Build AsgiRequest (multiple string allocations)
    let asgi_req = AsgiRequest {
        id: uuid.clone(),
        method: parts.method.as_str().to_string(),  // allocation
        path: parts.uri.path().to_string(),          // allocation
        query: parts.uri.query().unwrap_or("").to_string(),
        headers: parts.headers.iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str()...))  // N allocations
            .collect(),
        body: body_bytes.to_vec(),  // allocation
        ...
    };

    // 4. MessagePack serialize (~10μs)
    let payload = rmp_serde::to_vec_named(&asgi_req)?;

    // 5. Another allocation for header + payload
    let mut message = Vec::with_capacity(RequestHeader::SIZE + payload.len());
    message.extend_from_slice(&header.to_bytes());
    message.extend_from_slice(&payload);

    // 6. Register in pending map (DashMap insert)
    worker.pending.insert(request_id, tx);

    // 7. Send to ring (memcpy + atomics)
    worker.send_request(&message, 0).await?;

    // 8. Notify Python (eventfd syscall)
    worker.req_notifier.notify()?;

    // 9. Wait for response (async oneshot channel)
    let response_data = tokio::time::timeout(timeout, rx).await;

    // 10. Deserialize response (~10μs)
    let asgi_resp: AsgiResponse = rmp_serde::from_slice(payload)?;

    // 11. Build HTTP response (header iteration)
    for (k, v) in asgi_resp.headers {
        builder = builder.header(k, v);
    }
}
```

**Identified bottlenecks:**
1. **11+ heap allocations** per request (UUID, strings, vectors)
2. **2 serialization cycles** (request + response)
3. **2 eventfd syscalls** (request notify + response notify)
4. **2 context switches** minimum (user→kernel→user for each notification)
5. **DashMap operations** for pending request tracking

### Python Side (`worker.py:688-719`, `1000-1117`)
```python
async def _handle_client_payload(self, client_id, client, payload):
    # 1. Parse header (24 bytes)
    header = RequestHeader.from_bytes(payload[:HEADER_SIZE])

    # 2. Deserialize request (~10μs)
    message = self._codec.decode(payload[HEADER_SIZE:])

    # 3. Create ResponseWriter with aggregation state
    writer = ResponseWriter(..., managed_mode=True)

    # 4. Handle request...

async def _run_http_request(self, meta, body_state, writer, contexts):
    # PERF optimizations already applied:
    # - LazyHeaders (defers encoding)
    # - Scope templates (reuses constant keys)
    # - ResponseHolder (avoids asyncio.Queue overhead)

    scope = {**_HTTP_SCOPE_TEMPLATE, ...}  # ~10 mutable keys

    # Run ASGI app
    await self._app(scope, receive, send)

    # Format and send response frames
    # In managed_mode, aggregates into single AsgiResponse
```

## Theoretical Performance Ceiling

### Best Case IPC Overhead
Even with perfect optimization:
- **2 memcpys** (request + response): ~2μs
- **2 atomic operations**: ~0.2μs
- **2 eventfd calls**: ~2μs (kernel bypassed with io_uring, but still overhead)
- **Minimum context switch latency**: ~10μs

**Theoretical floor**: ~15-20μs additional per request

### Practical Ceiling Calculation
```
Granian RPS × (Granian latency / (Granian latency + IPC overhead))
= 62,364 × (120μs / (120μs + 80μs))
= 62,364 × 0.6
= ~37,400 RPS theoretical max
```

With current implementation (~100-130μs IPC overhead):
```
62,364 × (120μs / (120μs + 130μs))
= 62,364 × 0.48
= ~30,000 RPS practical max
```

**Current achieved: 14,444 RPS** (48% of practical max)

## Why Vertebra is Below Practical Max

The 2x gap between practical max (30K) and actual (14K) suggests:

1. **Excessive allocations**: 11+ heap allocations per request
2. **UUID overhead**: `uuid::Uuid::new_v4().to_string()` is ~48 bytes + random gen
3. **String conversions**: Every header key/value converted to owned String
4. **Serialization overhead**: MessagePack with named fields adds ~30% overhead
5. **Python asyncio overhead**: `asyncio.create_task()` per request
6. **P99 tail latency**: 36.9ms suggests occasional GC pauses or lock contention

## Optimization Paths

### Path A: Optimize Within IPC Architecture (Target: 30-40K RPS)

1. **Eliminate allocations in hot path**:
   - Use integer request IDs instead of UUID strings
   - Zero-copy headers (pass references, not owned Strings)
   - Pre-allocated message buffers

2. **Reduce serialization**:
   - Use positional MessagePack (not named fields)
   - Or switch to zero-copy format (Cap'n Proto, FlatBuffers)

3. **Batch notifications**:
   - Current: Notify per request
   - Better: Coalesce multiple requests into single notification

4. **io_uring for notifications** (Linux 5.1+):
   - Eliminates eventfd syscall overhead
   - ~0.5μs vs ~1μs per notification

### Path B: Hybrid In-Process Model (Target: 50-60K RPS)

Run Python interpreter in-process via PyO3 for simple handlers:

```rust
// Simple handlers: direct PyO3 call
if is_simple_handler(path) {
    let result = Python::with_gil(|py| {
        handler.call1(py, (request,))
    })?;
    return build_response(result);
}

// Complex handlers: IPC to worker
dispatch_to_worker(req, worker).await
```

**Trade-offs**:
- Pros: Near-Granian performance for simple handlers
- Cons: GIL contention, more complex architecture, loses process isolation

### Path C: Accept IPC Trade-offs, Focus on Other Value

IPC provides benefits that may justify lower RPS:
- **Process isolation**: Python crash doesn't kill server
- **Independent scaling**: Workers can be scaled separately
- **Memory isolation**: No GIL contention between requests
- **Multi-language support**: Easy to add Node.js/Go workers

## Recommendation

**Short-term (1-2 weeks)**: Pursue Path A optimizations
- Replace UUID with atomic u64 counter (already done: `next_request_id()`)
- Eliminate string allocations in bridge.rs
- Use positional MessagePack encoding
- Target: **25-30K RPS** (75-100% improvement)

**Medium-term**: Evaluate hybrid model for benchmarks
- Implement PyO3 fast path for "hello world" scenarios
- Maintain IPC path for production workloads
- Target: **50K+ RPS** for simple handlers

**Long-term**: Accept architectural trade-off
- Document IPC overhead clearly
- Position Vertebra for reliability/isolation, not raw speed
- Compare against Uvicorn H11 (14K vs 11K = 30% faster already) and target httptools parity

## Conclusion

The **100K-200K RPS target is not achievable** with the current IPC architecture. The theoretical ceiling is ~37K RPS, and practical max is ~30K RPS.

However:
1. **14K → 30K** is achievable with optimization (2x improvement)
2. **30K → 50K** may be achievable with hybrid PyO3 mode
3. Vertebra already **beats Uvicorn (h11)** by 30%, but trails **Uvicorn (httptools)** by 50%, while providing process isolation

The question becomes: Is process isolation worth the ~2x performance penalty vs Granian?
