# Research: Re-imagining Native Mode for State-of-the-Art Performance

## Executive Summary

- **Current Status**: Vertebra's "Native" mode is currently an **Optimized ASGI Adapter**. It improves on standard ASGI by using `ResponseHolder` (bypassing `asyncio.Queue`), `LazyHeaders`, and `PipelineCompiler` (JIT-compiling Litestar guards/DI).
- **The Gap**: Even with these optimizations, it still pays the **Per-Request Tax**: one UUID per request, one `asyncio.create_task` per request, and one FFI boundary crossing per request. WSGI is faster because it's a simple, synchronous, one-call-per-request protocol that Litecore highly specialized.
- **Re-imagined Native**: To exceed WSGI (~200k+ req/s), Native mode must move from a "Per-Request" model to a **Batch-Oriented "Ghost Framework"** model.
- **Key Verdict**: Native mode is absolutely worth keeping, but it should be re-imagined not as an *alternative* to ASGI, but as the **Underlying High-Performance Transport** that ASGI and WSGI eventually emulate for compatibility.

---

## 1.0 Current Native vs. Re-imagined Native

| Feature | Current Native (Vertebra) | Re-imagined Native (The Frontier) |
|---------|---------------------------|-----------------------------------|
| **Boundary Crossing** | 1 Call per Request | **Vectorized FFI**: 1 Call per Batch (10-100 requests). |
| **Request Object** | `msgspec.Struct` (Copy) | **Pointer-Stable Proxy**: Python receives a C-pointer; data is read directly from SHM on-demand. |
| **Routing** | Rust -> ID -> Python Runner | **AOT Compiled Dispatch**: Rust identifies the route and passes a direct function pointer to the JIT runner. |
| **Response Body** | Python -> SHM -> Broker | **Zero-Copy Handover**: Python returns a "Data Handle"; Broker reads from SHM/Cache directly to socket. |
| **Event Loop** | `asyncio` per Request | **Synchronous Fast-Path**: If the handler is `def` (not `async def`), it's executed immediately in the batch loop. |

---

## 2.0 Is Native Mode Worth Keeping?

**YES.** Standard protocols (ASGI/WSGI) are built for generic servers (Uvicorn, Gunicorn). Vertebra is a **Polyglot System** with a Shared Memory backplane. Native mode is the only way to exploit the unique advantages of this architecture:

1. **Shared Memory Pointers**: Standard protocols don't know about `/dev/shm`. Native mode can pass raw pointers between Rust and Python.
2. **Batching**: ASGI is strictly per-request. Native mode can handle 50 requests in a single GIL-acquisition window.
3. **Rust-Level Authorization**: Native mode allows the Rust server to handle the "Outer Shell" (Auth, Rate Limiting, CORS) and only "wake" Python for the specific business logic.

---

## 3.0 How to Exceed WSGI Performance

WSGI's performance limit is ~200k req/s because it still requires one Python call per request. To go beyond this, we must use **Batching**.

### The "Batch Native" Re-imagination:
Instead of:
```python
# Current Worker Loop
for msg in ring.recv():
    asyncio.create_task(app(scope, receive, send))
```

We move to:
```python
# Re-imagined Batch Runner
@vertebra.native_batch
def process_batch(requests: list[VertebraRequestProxy]):
    # 1. Rust has already validated/authorized these 100 requests.
    # 2. We process them in a single synchronous loop.
    # 3. We return a batch of results.
    return [handler(r) for r in requests]
```

### Why this beats WSGI:
1. **FFI Amortization**: Crossing from Rust to Python takes ~1-2µs. Doing it 100 times per batch instead of 100 times per request saves ~150µs of overhead.
2. **Zero Object Creation**: By using a `Proxy` object (like `bungi`'s Zig implementation), we don't create 100 `dict` objects for `scope`. We create **zero** objects until a field is actually touched.
3. **No UUIDs**: The Rust server uses internal `uint64` IDs. Converting these to `String` UUIDs in Python is a hidden performance killer.

---

## 4.0 Feature Expansion in Re-imagined Native

Native mode can support features that WSGI/ASGI cannot easily provide:

1. **Structured Streaming**: Direct integration with **Apache Arrow**. The Python worker can return an Arrow Table handle, and the Rust server streams it out as a high-performance binary or JSON stream.
2. **Backpressure Signaling**: Native mode can use the `AsyncRing` depth to signal the Rust server to slow down *before* the Python worker hits OOM.
3. **Atomic State Updates**: Python can update "Shadow State" in shared memory that the Rust server reads instantly for load balancing or metrics.

---

## 5.0 Conclusion & Recommendation

Native mode should be kept and elevated to the **primary internal protocol**. 

- **Phase 1**: Implement the **Vectorized FFI** (Batch delivery of requests).
- **Phase 2**: Replace `msgspec` serialization for requests with **Zero-Copy Proxy Objects** (PyO3 handles pointing to SHM).
- **Phase 3**: Implement the **Sync-Fast-Path** to bypass `asyncio` for non-async handlers.

This re-imagination turns Vertebra from a "fast ASGI server" into a **"Native Data-Plane Accelerator"** for Python.
