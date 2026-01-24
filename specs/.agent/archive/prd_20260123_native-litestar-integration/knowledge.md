# Knowledge Capture: Native Litestar Integration (Direct Protocol)

## Patterns Discovered
- **Route Reflection:** Using `app.routes` and traversing `HTTPRoute` objects allows us to build a comprehensive map of the application structure without executing request logic.
- **Dynamic Compilation:** Using `exec` to generate Python functions (`PipelineCompiler`) is significantly faster than using `functools.partial` or class-based wrappers for the hot path, as it removes Python stack frames.
- **Hybrid Routing:** Using `matchit` in Rust for high-performance routing while keeping standard ASGI fallback allows for safe, incremental adoption.

## Lessons Learned
- **Type Safety in FFI:** Manually handling struct serialization between Rust and TS/Python via JSON/MsgPack is error-prone. We should consider generating shared types (e.g., using `typeshare` or `ts-rs`).
- **Litestar Internals:** Path parameters are not automatically injected when bypassing `ASGIConnection`. We had to explicitly extract them from the router match and inject them into the handler.
- **Testing:** Mocking the full request lifecycle for "Direct" mode required simulating both the IPC message format and the internal Vertebra objects (`VertebraRequest`, `VertebraWebSocket`).
- **Bun FFI Strings:** Passing strings to C ABI via Bun `dlopen` requires manual null-termination and buffer encoding (`Buffer.from(s + '\0')`) if using `ptr` type, or relying on `cstring` which seemed flaky in our tests.

## Decisions Made
- **Bypass `ASGIConnection`:** We chose to implement `VertebraRequest` instead of subclassing `ASGIConnection` to avoid the overhead of its state management and scope validation.
- **`matchit` for Routing:** We selected `matchit` over `radix-trie` for its Rust performance and ecosystem support (used by Axum, Salvo).
- **JSON for Param Passing:** We serialize path parameters to JSON in the C ABI for simplicity in the TS broker, accepting a slight serialization cost for the flexibility of handling dynamic param sets.

## Reusable Components
- `PipelineCompiler`: Can be adapted for other frameworks (FastAPI, etc.) by changing the reflection logic.
- `VertebraRequest`: A reusable, high-performance request wrapper that lazy-loads data.
- `ShmRouter`: A standalone, FFI-friendly wrapper around `matchit`.

## Gotchas & Warnings
- **Path Param Types:** Currently, `PipelineCompiler` treats all path parameters as strings or ints (if annotated). Complex converters/transformers are not yet supported in the native path.
- **Dependency Scope:** We assume dependencies are thread-safe or request-scoped. Application-state dependencies work fine, but sophisticated lifespan management for dependencies is simplified compared to full Litestar.
