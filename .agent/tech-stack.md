# Technology Stack: Vertebra

## Languages & Runtimes

- **Python 3.12+**: Primary application and business logic layer. Managed via `uv`.
- **Rust 2021**: Core performance layer for IPC primitives and shared memory management. Uses PyO3 for Python bindings and Napi-rs for JS/TS bindings.
- **TypeScript / Bun**: High-performance networking, broker logic, and frontend integration.

## Core Architecture

- **Zero-Copy Data Plane**: Shared-memory ring buffers (SPSC/MPMC) and Arrow IPC for high-throughput, low-latency data transfer.
- **IPC Mechanism**: Shared-memory C ABI (no sockets/TCP in the data plane).
- **Process Model**: Centralized Bun broker managing shared resource pools across Python worker processes. `ProcessManager` handles worker lifecycle with health checks and exponential backoff restarts.
- **Authentication**: Bun-first JWT/OAuth2/IAP validation at HTTP ingress with JWKS caching. Claims flow to Python via IPC. Python provides `LazyUserProxy` for deferred user loading and context-based access via `vertebra.auth.state`.
- **Security Ops**: Password hashing (Argon2/Bcrypt), CSRF signing, and cryptographic random delegated to Bun broker via dedicated opcodes.
- **Native Integration**: Direct execution path for Litestar applications bypassing ASGI. `PipelineCompiler` pre-compiles guards and DI graphs; `VertebraRequest` provides lazy-loaded headers/JSON.
- **Router**: Rust-based high-performance router (`matchit`) shared between Bun broker and Python workers.
- **MCP Runtime**: Model Context Protocol implementation (Stdio, SSE, WebSocket) for AI agent integration. IPC bridge routes tool calls to Python workers.
- **Observability**: Prometheus-formatted metrics registry with collectors for broker pools, HTTP server, and MCP tool execution.
- **Native State**: Real-time state synchronization primitive (`NativeState`) with auto-broadcast and snapshot copying for thread safety.

## Backend Frameworks

- **Litestar**: Python-first API framework, utilized for its performance and type-safe DI system.

## Data & Storage

- **SQLSpec**: Multi-dialect SQL query mapper with broker-backed adapters and optimized ADK/Litestar store extensions.
- **Apache Arrow**: Primary serialization format for large tabular datasets (>1k rows).
- **SQLAlchemy / Advanced Alchemy**: Native dialects and Repository/Service patterns integrated with the shared-memory broker.
- **Storage**: Unified object store abstraction compatible with `fsspec`, supporting local, S3, and GCS backends via the broker.

## Tooling & Infrastructure

- **Dependency Management**: `uv` (Python), `bun` (TypeScript/JS), `cargo` (Rust).
- **Task Runner**: Makefile for cross-language build, test, and lint orchestration.
- **Testing**: `pytest` (Python), `bun test` (TS), `cargo test` (Rust).
- **Linting/Formatting**: `ruff` (Python), `biome` (TypeScript), `rustfmt`/`clippy` (Rust).
- **Distribution**: `python-build-standalone` (PBS) for portable Python runtimes; `vertebra build` for single-file executables.
