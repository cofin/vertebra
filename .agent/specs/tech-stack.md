# Technology Stack: Vertebra

## Languages & Runtimes

- **Python 3.12+**: Primary application and business logic layer. Managed via `uv`.
- **Rust 2021**: Core performance layer for IPC primitives and shared memory management. Uses PyO3 for Python bindings and Napi-rs for JS/TS bindings.
- **TypeScript / Bun**: High-performance networking, broker logic, and frontend integration.

## Core Architecture

- **Zero-Copy Data Plane**: Shared-memory ring buffers (SPSC/MPMC) and Arrow IPC for high-throughput, low-latency data transfer.
- **IPC Mechanism**: Shared-memory C ABI (no sockets/TCP in the data plane).
- **Process Model**: Centralized Bun broker managing shared resource pools across Python worker processes.

## Backend Frameworks

- **Litestar**: Python-first API framework, utilized for its performance and type-safe DI system.

## Data & Storage

- **SQLSpec**: Multi-dialect SQL query mapper with broker-backed adapters.
- **SQLAlchemy / Advanced Alchemy**: Native dialects and Repository/Service patterns integrated with the shared-memory broker.
- **Storage**: Unified object store abstraction compatible with `fsspec`, supporting local, S3, and GCS backends via the broker.

## Tooling & Infrastructure

- **Dependency Management**: `uv` (Python), `bun` (TypeScript/JS), `cargo` (Rust).
- **Task Runner**: Makefile for cross-language build, test, and lint orchestration.
- **Testing**: `pytest` (Python), `bun test` (TS), `cargo test` (Rust).
- **Linting/Formatting**: `ruff` (Python), `biome` (TypeScript), `rustfmt`/`clippy` (Rust).
