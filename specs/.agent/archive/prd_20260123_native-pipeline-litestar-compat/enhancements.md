# Native Pipeline Enhancements & Technical Constraints

## Focus: Multipart/Form-Data Handling

This document outlines the specific technical challenges preventing immediate native support for `multipart/form-data` (file uploads) in the Vertebra Native Pipeline, necessitating the current strategy of **ASGI Fallback**.

### 1. The Core Architecture Conflict

Vertebra's Native Pipeline is designed for **Zero-Copy** efficiency using Shared Memory (SHM) ring buffers. This architecture is optimized for payloads that fit within a reasonable buffer size (e.g., JSON RPC messages).

**Multipart Uploads** fundamentally conflict with this model because:
1.  **Arbitrary Size:** File uploads can be gigabytes in size, far exceeding any reasonable SHM ring buffer slot (typically 64KB - 1MB).
2.  **Streaming Necessity:** To handle large files, data *must* be streamed. We cannot buffer the entire payload in RAM/SHM before processing.
3.  **Broker Bottleneck:** Passing gigabytes of file data through the central Bun Broker into Python workers via SHM would saturate the ring buffers, blocking all other control signals and RPC calls (like health checks or small API requests).

### 2. The Parsing Complexity

In a standard ASGI setup, the ASGI server (Uvicorn/Granian) handles the raw socket stream and provides an async iterator (`receive()`) that yields chunks. Litestar consumes these chunks to parse multipart boundaries.

In the **Native Pipeline**, we bypass the ASGI `receive()` loop. To support multipart natively:

1.  **Bun Side:** The Bun broker receives the HTTP stream. It would need to either:
    *   Buffer the whole file (OOM risk).
    *   Stream chunks over IPC.
2.  **IPC Bridge:** We would need a new IPC opcode (`STREAM_CHUNK`) to feed data incrementally to the Python worker.
3.  **Python Side:** We would need to re-implement a multipart parser that accepts these IPC chunks and reconstructs the `UploadFile` abstraction Litestar expects.
    *   *Challenge:* Litestar's `Request` object expects an ASGI scope and a `receive` awaitable. We would have to mock the entire ASGI `receive` channel using IPC messages to trick Litestar into parsing it, or write a custom `NativeMultipartParser`.

### 3. Missing Infrastructure

To support this *performantly* (without copying data 3 times: Socket -> Bun -> SHM -> Python), we are missing two critical pieces of infrastructure:

1.  **Side-Channel Data Transfer:** A mechanism for the Bun broker to write file data directly to a temporary file on disk (or a separate large-buffer SHM region) and pass a *handle* (path or descriptor) to Python, rather than streaming bytes through the command ring buffer.
2.  **Rust-Based Streaming Parser:** Ideally, the parsing should happen in Rust (`vertebra-core`), stripping the multipart boundaries and providing a clean file descriptor or memory map to Python. We currently lack this implementation.

### 4. ROI & Fallback Strategy

**Cost:** High. Requires implementing a streaming IPC protocol and custom multipart parsers in both TS and Python.
**Benefit:** Minimal latency gain for large files (network I/O is the bottleneck, not IPC overhead).

**Decision:**
For Phase 1, we use **ASGI Fallback**.
*   **Mechanism:** When the route capability classifier detects `RequestEncodingType.MULTI_PART` or `UploadFile` dependencies, the route is excluded from the Native Router.
*   **Result:** The Bun broker sees no native route match and dispatches the request via the standard `proxy_request` (ASGI) path.
*   **Outcome:** Correctness is guaranteed. Performance is "standard ASGI" (which is acceptable for uploads), and the high-speed Native ring buffers are kept clear for latency-sensitive RPC/API traffic.
