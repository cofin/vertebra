# Vertebra

Vertebra is a **Python-first, high-performance application backbone** that combines:

- Python developer experience with type-safe APIs.
- Rust IPC primitives for zero-copy data transfer.
- Bun/TypeScript server tooling for high-throughput networking.

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
make js-test       # Run TypeScript tests
make js-type-check # Run TypeScript type checks
make test          # Run Python tests
make lint          # Run all linters
```

## Repository Layout

```
src/
  py/   # Python package
  rs/   # Rust workspace
  ts/   # Bun/TypeScript workspace
```

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

PRD-0 is complete and PRD-1 (Rust IPC core) is now in progress. See
`docs/progress.md` for current status.

## IPC Example

```bash
uv run python tools/ipc_producer.py --name /vertebra-demo
uv run python tools/ipc_consumer.py --name /vertebra-demo
```
