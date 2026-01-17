# Vertebra

Vertebra is a **Python-first, high-performance application backbone** that combines:

- Python developer experience with type-safe APIs.
- Rust IPC primitives for zero-copy data transfer.
- Bun/TypeScript server tooling for high-throughput networking.

## Quickstart

```bash
make install
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
- Progress + next steps: `docs/progress.md`
- PRDs: `docs/prd/`

## Status

PRD-0 foundation work is in progress. See `docs/progress.md` for current status
and `docs/prd/PRD-0-foundation.md` for the detailed plan.
