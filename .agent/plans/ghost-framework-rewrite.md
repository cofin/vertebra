# Plan: Vertebra 0.1.0 "Ghost Framework" Rewrite

This plan outlines the complete reconstruction of Vertebra into a state-of-the-art, high-performance web backbone. It incorporates lessons from Litecore, Spikard, and Bungi, moving to a Vectorized FFI and Zero-Object Proxy architecture.

## Roadmap Overview (22 Flows)

### Saga 1: Clean Slate & Foundation
1. **Clean Slate Wipe (`wipe-slate_20260314`)**: Archive current code to `/scratch/`.
2. **Scaffold Base 0.1.0 (`scaffold-base_20260314`)**: `uv` and `cargo` workspace initialization.

### Saga 2: Application Shell
3. **CLI Entrypoints & Banners (`cli-entrypoints_20260314`)**: `vertebra` CLI and branding.
4. **VertebraApp Python API (`app-interface_20260314`)**: User-facing Python app and routing API.
5. **Rust Supervisor & Worker Lifecycle (`supervisor-lifecycle_20260314`)**: Rust-side worker management.

### Saga 3: Native Core (Rust)
6. **Native IPC Substrate (`ipc-substrate_20260314`)**: SHM, Ring Buffers, and Arena allocators.
7. **Native HTTP Server - H2 & TLS (`http-server-h2_20260314`)**: Hyper-based H2 server.
8. **HTTP/3 & QUIC Transport (`http3-quic-transport_20260314`)**: Native H3 support.
9. **Native Routing & Dispatch (`native-routing_20260314`)**: matchit routing to Handler IDs.
10. **Batch Collector (`batch-collector_20260314`)**: Aggregating requests into batches.
21. **Data Plane Bypass (`data-plane-bypass_20260314`)**: Zero-copy SHM-to-Socket streaming.

### Saga 4: FFI Bridge
11. **PyO3 Request Proxy (`pyo3-request-proxy_20260314`)**: Zero-object Python proxy.
12. **Vectorized FFI API (`vectorized-ffi-api_20260314`)**: Batch polling/submission endpoints.
13. **Python Batch Runner (`python-batch-runner_20260314`)**: Optimized execution loop.

### Saga 5: Protocol Adapters
14. **WSGI Core Adapter (`wsgi-core-adapter_20260314`)**: Lazy WSGI emulation.
15. **WSGI Extension Support (`wsgi-extension-support_20260314`)**: Django/Flask hooks.
16. **ASGI Core Adapter (`asgi-core-adapter_20260314`)**: Scope builder and async runner.
17. **ASGI Extension Support (`asgi-extension-support_20260314`)**: FastAPI/Litestar pipelines.

### Saga 6: Frontend Integration
18. **Build Orchestrator (`build-orchestrator_20260314`)**: Rolldown/Vite integration.
19. **Type Bridge & Codegen (`type-bridge-codegen_20260314`)**: TS/Zod emission.
20. **HMR & Developer Experience (`hmr-dx-experience_20260314`)**: Websocket signaling.
22. **Vertebra PyClient SDK (WASM) (`vertebra-pyclient-wasm_20260314`)**: PyScript/Pyodide SDK.

## Implementation Details
Each flow will be created as a standard Flow specification in `.agent/specs/` with:
- `spec.md`: Detailed plan and tasks.
- `metadata.json`: Tracking data.
- `learnings.md`: Append-only log.

## Verification
- **Performance Gates**: Benchmarks will be run after Saga 3 and 4 to ensure 200k+ req/s.
- **Protocol Parity**: Integration tests for Django and FastAPI will ensure compatibility.
