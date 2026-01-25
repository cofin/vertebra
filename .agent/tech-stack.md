# Technology Stack: Vertebra

## Languages & Runtimes

- **Python 3.12+**: Primary application and business logic layer. Managed via `uv`.
- **Rust 2021**: Core performance layer for IPC primitives and shared memory management. Uses PyO3 for Python bindings and Napi-rs for JS/TS bindings.
- **TypeScript / Bun / Node.js**: High-performance networking, broker logic, and frontend integration.

## Core Architecture

- **Zero-Copy Data Plane**: Shared-memory ring buffers (SPSC/MPMC) and **Apache Arrow** for high-throughput, low-latency data transfer.
- **IPC Mechanism**: Shared-memory C ABI (no sockets/TCP in the data plane).
- **Process Model**: Centralized Broker managing shared resource pools across Python worker processes. `ProcessManager` handles worker lifecycle with health checks and exponential backoff restarts.
- **Authentication**: Broker-first JWT/OAuth2/IAP validation at HTTP ingress with JWKS caching. Claims flow to Python via IPC.
- **Native Integration**: Direct execution path for **Litestar** applications bypassing ASGI. `PipelineCompiler` pre-compiles guards and DI graphs; `VertebraRequest` provides lazy-loaded headers/JSON.
- **Router**: Rust-based high-performance router (`matchit`) shared between Broker and Python workers.
- **MCP Runtime**: Model Context Protocol implementation (Stdio, SSE, WebSocket) for AI agent integration. IPC bridge routes tool calls to Python workers.

## Backend Frameworks

- **Litestar**: Python-first API framework, utilized for its performance and type-safe DI system.
- **Native Procedures (tRPC-like)**: Type-safe remote procedure calls with automatic backend-to-frontend type bridging.
  - **Procedures Registry**: `vertebra.procedures` handles registration, metadata extraction, and serialization detection (msgspec/Pydantic).
  - **Native State**: Real-time state synchronization primitive (`NativeState`) with auto-broadcast, snapshot copying, and subscription support (similar to Reflex).
  - **Streaming**: Native support for `AsyncIterable` via `StreamingResponse` (NDJSON) for real-time updates.

## Data & Storage

- **SQLSpec**: Multi-dialect SQL query mapper with broker-backed adapters and optimized ADK/Litestar store extensions.
- **Apache Arrow**: Primary serialization format for large tabular datasets (>1k rows).
- **SQLAlchemy / Advanced Alchemy**: Native dialects and Repository/Service patterns integrated with the shared-memory broker.
- **Storage**: Unified object store abstraction compatible with `fsspec`, supporting local, S3, and GCS backends via the broker.

## Frontend Tooling & Integration

- **Framework Agnostic Helpers**: Utilities and adapters to streamline integration with popular frontend frameworks (e.g., React, Vue, Svelte, Angular).
- **Type Generation**: Automatic generation of TypeScript types/interfaces from Python models for end-to-end type safety.
- **RPC Clients**: Typed RPC clients for seamless communication with Vertebra backends.

## Tooling & Infrastructure

- **Dependency Management**: `uv` (Python), `bun` (TypeScript/JS), `cargo` (Rust).
- **Task Runner**: Makefile for cross-language build, test, and lint orchestration.
- **Testing**: `pytest` (Python), `bun test` (TS), `cargo test` (Rust).
- **Linting/Formatting**: `ruff` (Python), `biome` (TypeScript), `rustfmt`/`clippy` (Rust).
- **Distribution**: `python-build-standalone` (PBS) for portable Python runtimes; `vertebra build` for single-file executables.