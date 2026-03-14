# Research: Clean Slate Inventory - The "Ghost Framework" Architecture

## Executive Summary

If we were to wipe the `vertebra` project clean and start from scratch with the sole goal of achieving state-of-the-art performance (200k+ req/s, exceeding specialized WSGI and Spikard), we would build the **"Ghost Framework"**.

This clean slate assumes:
1. **Rust is the Main Event**: The HTTP server, Router, Protocol Parsers, and Shared Memory Broker are entirely in Rust.
2. **Vectorized FFI**: Python is invoked via PyO3 in batches (1 call per N requests).
3. **Zero-Object Proxy**: Python receives pointers to Rust memory, extracting fields only on demand.
4. **Data-Plane Bypass**: Python returns metadata; Rust streams the actual bodies (files, DB results) from SHM to the socket.

---

## The Clean Slate Inventory

### 1. Rust Workspace (`src/rs/`)

The Rust side shrinks down to highly focused crates, abandoning the attempt to emulate ASGI per-request inside Rust.

#### `vertebra-core` (The IPC Substrate)
*Responsible for the pure data structures that cross the language barrier.*
* `src/shm.rs`: Shared memory region management (mmap).
* `src/ring.rs`: Lock-free SPSC and MPMC ring buffers (the queues).
* `src/batch.rs`: Batch node allocators (Arena allocators for zero-copy strings).
* `src/proxy.rs`: C-ABI compatible structs representing HTTP Requests and Responses.

#### `vertebra-http` (The Native Web Server)
*Responsible for receiving bytes from the NIC and organizing them into batches.*
* `src/server.rs`: Hyper/Tokio accept loop.
* `src/router.rs`: Native Rust router (e.g., matchit) that maps URLs to Python Handler IDs.
* `src/collector.rs`: The "Batcher" - gathers incoming Hyper requests into `BatchNode` arenas.
* `src/dispatch.rs`: Pushes batches to the `vertebra-core` ring buffers.
* `src/response.rs`: Reads responses from the ring buffer. Handles "Direct-to-Socket" SHM streaming if Python returned a body handle instead of bytes.

#### `vertebra-py` (The FFI Bridge)
*The PyO3 C-Extension. Compiles to `vertebra.core.so`.*
* `src/lib.rs`: PyO3 module definition.
* `src/request_proxy.rs`: Python heap type `VertebraRequest` that holds a pointer to a `vertebra_core::proxy::Request` in the arena. Implements lazy property getters (`.method`, `.path`, `.headers`).
* `src/batch_api.rs`: Exposes `get_batch()` to Python, which pops a `BatchNode` from the ring and returns a `List[VertebraRequest]`.
* `src/response_api.rs`: Exposes `submit_batch()` to Python, pushing results back to Rust.

---

### 2. Python Workspace (`src/py/vertebra/`)

The Python side becomes drastically thinner. It is no longer an ASGI server; it is a **Native Batch Runner** that *can* emulate ASGI if requested.

#### `vertebra.core` (The Engine)
*The highly optimized batch loop.*
* `_native.py` / `core.so`: The compiled PyO3 extension.
* `runner.py`: The core execution loop.
  ```python
  # Conceptual runner.py
  import vertebra.core as ffi

  def run_worker(router_map: dict[int, callable]):
      while True:
          batch = ffi.get_batch()  # List of zero-copy proxy objects
          results = []
          for req in batch:
              handler = router_map[req.handler_id]
              # Synchronous fast-path is the default
              results.append(handler(req)) 
          ffi.submit_batch(results)
  ```

#### `vertebra.web` (The Framework Surface)
*Minimal routing and application constructs.*
* `app.py`: `VertebraApp` definition. Registers routes and compiles the `router_map`.
* `routing.py`: Route definitions that sync their paths to the Rust `vertebra-http` router at startup.
* `requests.py`: Type hints and Python-side helpers for the `VertebraRequest` proxy object.
* `responses.py`: High-performance response types (e.g., `ShmFileResponse`, `ArrowResponse`, `JsonResponse`).

#### `vertebra.asgi` (The Compatibility Layer)
*Optional adapter for legacy ecosystem compatibility.*
* `adapter.py`: A specialized runner that takes a `VertebraRequest` proxy, inflates it into an ASGI `scope` dict, creates `asyncio.Queue` for `receive`/`send`, and calls standard ASGI apps (like FastAPI or standard Litestar). Used *only* for non-native routes.

#### `vertebra.extensions.litestar`
*Tight integration with Litestar.*
* `plugin.py`: Extracts Litestar's route tree, identifies which routes are "Native Compatible" (no complex dependencies, pure JSON/bytes returns), and registers them directly with the Rust router.
* `compiler.py`: JIT-compiles Litestar guards and DI into synchronous or highly optimized async functions that accept the `VertebraRequest` proxy directly, bypassing Litestar's internal ASGI layers.

---

## Files to be DELETED/REPLACED in a Clean Slate

If we execute this wipe, the following conceptual areas from the current repo would be eliminated:

1. **The ASGI-First Rust Bridge**: `src/rs/vertebra-server/src/bridge.rs` (which serializes standard ASGI dicts over messagepack) is deleted. Replaced by the `vertebra-py` zero-object proxy.
2. **MessagePack Overhead**: `src/py/vertebra/core/worker.py`'s heavy use of `msgspec.convert` and `MessagePackCodec` per request is deleted. Replaced by direct memory reads.
3. **Per-Request Asyncio Tasks**: The `asyncio.create_task` per incoming message in `worker.py` is deleted. Replaced by the synchronous/batch runner loop.
4. **Heavy Python State**: The `RequestContext`, `RequestBodyState`, and `ResponseHolder` objects in Python are mostly eliminated. State lives in the Rust Arena until the batch is complete.

## Summary of the Ideal Codebase
The ideal codebase has a **thick, highly concurrent Rust layer** and a **thin, synchronous-preferred Python layer**. Python is relegated strictly to business logic execution and application wiring, communicating with Rust exclusively through batched memory pointers.
