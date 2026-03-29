# Vertebra

Vertebra is a **Python-first, high-performance application backbone** that combines:

- Python developer experience with type-safe APIs.
- Rust IPC primitives for zero-copy data transfer.
- Rust-native server layer for high-throughput networking.

## Quickstart

```bash
make install
```

## Rust Build Instructions

```bash
cd src/rs
cargo build
cargo test
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
```

## Common Commands

```bash
make rs-build      # Build Rust workspace
make rs-test       # Run Rust tests
make test          # Run Python tests
make lint          # Format Python/Rust (ruff + rustfmt)
make lint-all      # Run full lint + type checks + pre-commit
```

## Repository Layout

```
src/
  py/   # Python package
  rs/   # Rust workspace
```

## Runtime Topologies

Vertebra supports two primary execution modes, selectable via the `--topology` flag:

### 1. Embedded Topology (Default)
- **Select with**: `--topology embedded` (or `--workers 0`)
- **Use Case**: Development and high-performance production on free-threaded Python (3.13t+).
- **Mechanism**: The Rust HTTP server and Python application run within the same process. Python is embedded via PyO3.
- **Benefits**: Zero IPC overhead, lowest possible latency, shared memory address space.

### 2. SHM Topology (Scale-out)
- **Select with**: `--topology shm`
- **Use Case**: Extreme scale-out, process isolation, or legacy multi-core support on non-free-threaded Python.
- **Mechanism**: The Rust HTTP server spawns independent Python worker processes. Requests and responses are transferred via high-speed Shared Memory (SHM) ring buffers with eventfd signaling.
- **Benefits**: Fault isolation, bypasses GIL bottlenecks on older Python versions, scales across many OS processes.

## Documentation

- Architecture: `docs/arch.md`
- Rust IPC core architecture: `docs/architecture/rust-ipc-core.md`
- Rust IPC getting started: `docs/architecture/rust-ipc-getting-started.md`
- Python IPC guide: `docs/architecture/python-ipc-guide.md`
- Performance tuning: `docs/architecture/performance-tuning.md`
- Error handling: `docs/architecture/error-handling.md`
- API reference: `docs/architecture/api-reference.md`
- Diagrams: `docs/architecture/diagrams.md`
- Progress + next steps: `docs/progress.md`
- PRDs: `docs/prd/`

## Status

PRD-0/PRD-1 are complete with PRD-2 through PRD-4 in active development. See
`docs/progress.md` for current status.

Integration tests will use a Docker/Podman runtime abstraction (no compose required)
to start ephemeral services as needed.

To run integration tests:
```bash
uv run pytest src/py/tests -m integration --integration
```

## IPC Example

```bash
uv run python tools/ipc_producer.py --name /vertebra-demo
uv run python tools/ipc_consumer.py --name /vertebra-demo
```
