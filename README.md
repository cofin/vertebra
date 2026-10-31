<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/banner-inline-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/banner-inline-light.svg">
    <img alt="Vertebra" src="docs/assets/banner-inline-light.svg" width="520">
  </picture>
  <p><strong>Python-first, high-performance polyglot application backbone powered by Rust 2024 and TypeScript.</strong></p>
</div>

---

Vertebra combines Python 3.12–3.15 (and free-threaded 3.14t/3.15t) developer ergonomics with a Rust 2024 runtime and a full-stack TypeScript SDK:

- **Python Framework & Multi-Protocol Runners**: Type-safe APIs (`App`, `Router`, `RouteDef`, `ProcedureRouter`, `Provide`, `QueueService`, Inertia v3, QuickJS SSR, MCP/A2A agent protocols, and end-to-end TypeScript/Zod codegen) plus native, ASGI 3.0, WSGI, and RSGI execution.
- **Rust Shared-Memory & IPC Runtime (`vertebra-core`, `vertebra-py`)**: Lock-free SPSC/MPMC shared-memory ring buffers, arena slot pools, and adaptive `futex`/`ulock` signaling for zero-copy request/response batch dispatch.
- **Rust HTTP Ingress & Bundler (`vertebra-http`, `vertebra-bundler`)**: Hyper + Tokio HTTP/1.1, HTTP/2, HTTP/3 (QUIC), gRPC framing, WebSocket, SSE, TLS, compression, static serving, embedded Rolldown/Oxc JavaScript bundling, and pooled QuickJS SSR.
- **Native Storage, Database Drivers & Client SDK (`vertebra-storage`, `vertebra-db`, `@vertebra/client`)**: Rust-backed object storage (S3, GCS, Azure, R2, OCI, HTTP, local), native database drivers, and a type-safe TypeScript client across React, Vue, Svelte, Angular, HTMX, and Vite.

## Quickstart

```bash
uv add vertebra-core
```

For local repository development:

```bash
make install
```

## CLI Usage (`vtb`)

```bash
vtb run myapp:app --host 127.0.0.1 --port 8000 --workers 4 --topology shm
vtb dev myapp:app --port 8000
vtb js build
vtb js generate-types
vtb init my-project
vtb build --standalone
```

## Common Make Targets

```bash
make dev         # Build Rust crates and install debug Python extensions
make build       # Build TS client, release Rust binaries, and wheel
make test        # Run all tests (Rust + Python + TypeScript)
make rs-test     # Run Rust workspace tests (tls + compression features)
make py-test     # Run Python test suite
make ts-test     # Run TypeScript client tests (Vitest)
make lint        # Format and lint Python, Rust, and TypeScript
make check       # Run full lint, type checks, all tests, and coverage
make docs        # Build Zensical site, postbuild, and verify artifacts
make docs-serve  # Serve Zensical documentation locally with live reload
make bench       # Run benchmark suite
```

## Repository Layout

```text
src/
  py/
    vertebra/          # Python framework, runners, CLI, agent, storage, db
    tests/             # Python unit and FFI test suite
  rs/
    vertebra-core/     # Lock-free MPMC/SPSC rings, futex/ulock, batch IPC
    vertebra-py/       # PyO3 native extension (_vertebra)
    vertebra-http/     # Hyper + Tokio HTTP/1.1, HTTP/2, HTTP/3, gRPC, TLS
    vertebra-bundler/  # Embedded Rolldown/Oxc bundler and QuickJS SSR
    vertebra-storage/  # Multi-cloud and local object storage (_storage)
    vertebra-db/       # Native Postgres, MySQL, and SQLite pools
  ts/
    vertebra-client/   # @vertebra/client TypeScript SDK
```

## Runtime Topologies

Vertebra supports two primary execution modes, selectable via `--topology`:

### 1. Embedded Topology (Default)
- **Select with**: `--topology embedded` (or `--workers 0`)
- **Use Case**: Development and high-performance production on single-process or free-threaded Python (3.14t/3.15t).
- **Mechanism**: The Rust HTTP server and Python application run within the same OS process via PyO3 and `BatchBridge`.
- **Benefits**: Zero IPC serialization overhead, lowest latency, shared memory address space.

### 2. SHM Topology (Scale-out)
- **Select with**: `--topology shm`
- **Use Case**: Multi-process scale-out and fault isolation across OS processes.
- **Mechanism**: The Rust HTTP server spawns independent Python worker processes connected via shared-memory SPSC ring pairs with `futex`/`ulock` and `eventfd` notification.
- **Benefits**: Process isolation, scales across multiple GIL-bound Python interpreter processes.
