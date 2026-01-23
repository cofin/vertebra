# Native Litestar Integration

Vertebra provides a high-performance "Native" integration for Litestar applications that bypasses the standard ASGI interface for HTTP and WebSocket requests.

## Overview

By leveraging Vertebra's shared-memory architecture and Litestar's introspection capabilities, we compile route handlers into optimized "runners" that:

1.  **Bypass the ASGI Event Loop**: Requests are dispatched directly from the Rust-based ring buffer to the handler function.
2.  **Zero-Copy Data Binding**: JSON payloads and path parameters are extracted lazily from the shared memory region.
3.  **Flattened Dependency Injection**: Guards and dependencies are resolved at startup and compiled into a linear execution path.

## Architecture

### 1. Route Reflection (Startup)
When a worker starts, it inspects the Litestar application instance:
-   Extracts all routes (HTTP and WebSocket).
-   Compiles a `RouteMap` mapping paths to handler IDs.
-   Sends this map to the Rust core and Bun broker.

### 2. The Router (Rust/Bun)
The Bun broker uses a high-performance Rust router (via `matchit`) to match incoming URLs.
-   If a match is found, the broker sends a `direct.http` (or `ws.open`) opcode to the worker.
-   If no match is found (or for middleware that requires it), it falls back to standard ASGI.

### 3. The Pipeline Compiler (Python)
For each native route, Vertebra generates a specialized `runner` function:
-   Inlines `sync` and `async` guards.
-   Pre-calculates dependency resolution order.
-   Injects `VertebraRequest` (a lightweight wrapper) instead of `ASGIConnection`.

## Usage

Native integration is enabled automatically when running a Litestar app with `vertebra serve`.

```bash
vertebra serve myapp:app
```

To disable it and force standard ASGI mode (e.g., for debugging):

```bash
VERTEBRA_NATIVE=0 vertebra serve myapp:app
```

## Performance

Native integration significantly reduces the per-request overhead, particularly for:
-   **JSON APIs**: Direct parsing and serialization.
-   **High-Concurrency WebSockets**: Bypassing the `while True: await receive()` loop.
