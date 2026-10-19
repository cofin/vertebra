# Vertebra

Vertebra is a **Python-first, high-performance application backbone** that combines:

- Python developer experience with type-safe APIs (`VertebraApp`, `Router`, Inertia v3, SSR, and end-to-end TypeScript/Zod codegen).
- Rust IPC primitives (`vertebra-ipc`, `vertebra-py`) for zero-copy batch and shared-memory request/response dispatch.
- Rust-native HTTP/1.1, HTTP/2, TLS, compression, and static/HMR server layer (`vertebra-http`) with embedded Rolldown/Oxc JavaScript bundling (`vertebra-bundler`).
- Multi-protocol support for native Vertebra handlers, ASGI 3.0 applications, and PEP 3333 WSGI applications.

## Quickstart

```bash
make install
```

## CLI Usage (`vtb`)

```bash
vtb run myapp:app --host 127.0.0.1 --port 8000 --workers 4 --protocol auto
vtb dev myapp:app --port 8000
vtb js build
vtb js codegen
vtb init my-project
```

## Common Make Targets

```bash
make dev           # Build Rust crates and install native Python extensions in debug mode
make build         # Build TypeScript client, release Rust binaries, and Python wheel
make test          # Run all tests (Rust + Python + TypeScript)
make rs-test       # Run Rust workspace tests (with tls and compression features)
make py-test       # Run Python test suite
make ts-test       # Run TypeScript client tests (Vitest)
make lint          # Format and lint Python (Ruff + Mypy), Rust (rustfmt + Clippy), and TS (Biome)
make check         # Run full lint, type checks, all tests, and coverage
make bench         # Run benchmark suite
```

## Repository Layout

```text
src/
  py/
    vertebra/      # Python framework, server runners (native/ASGI/WSGI), builder/codegen, CLI, and supervisor
    tests/         # Python unit and FFI test suite
  rs/
    vertebra-ipc/      # Lock-free MPMC/SPSC shared-memory ring buffers, eventfd signaling, and batch protocol
    vertebra-py/       # PyO3 native extension (VertebraBatch, VertebraRequest, VertebraResponse, AsgiScope, WsgiEnviron)
    vertebra-http/     # Hyper + Tokio HTTP/1.1 & HTTP/2 server, radix router, TLS, compression, static files, and HMR
    vertebra-bundler/  # Embedded Rolldown bundler and QuickJS SSR pool
  ts/
    vertebra-client/   # @vertebra/client TypeScript SDK (typed fetch client, Inertia helpers, HMR client)
```

## Runtime Topologies

Vertebra supports two primary execution modes, selectable via `--topology`:

### 1. Embedded Topology (Default)
- **Select with**: `--topology embedded` (or `--workers 0`)
- **Use Case**: Development and high-performance production on single-process or free-threaded Python (3.13t+).
- **Mechanism**: The Rust HTTP server and Python application run within the same OS process via PyO3 and `BatchBridge`.
- **Benefits**: Zero IPC serialization overhead, lowest latency, shared memory address space.

### 2. SHM Topology (Scale-out)
- **Select with**: `--topology shm`
- **Use Case**: Multi-process scale-out and fault isolation across OS processes.
- **Mechanism**: The Rust HTTP server spawns independent Python worker processes connected via shared-memory SPSC ring pairs with `eventfd` notification.
- **Benefits**: Process isolation, scales across multiple GIL-bound Python interpreter processes.
