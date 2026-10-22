---
title: "Testing & Verification Index"
description: "Index of Vertebra's testing, verification, and benchmark chapters across Rust, Python, TypeScript, and free-threaded Python 3.14t."
status: stable
tags:
  - testing
  - index
  - verification
  - benchmarks
---

# Testing & Verification Index

- [Polyglot Test Matrix & Environment Setup](polyglot-test-matrix.md) — `make setup-env` (`tools/scripts/setup-env.sh`), `make dev` (`_vertebra`, `_bundler`, `_storage`), `make rs-test`, `make py-test`, `make ts-test`, `make lint-all`, `make check`, CI workflow matrix (`.github/workflows/ci.yml`: Python `3.12`–`3.14t`, Rust feature matrix), and `/dev/shm` test hygiene.
- [Benchmark Suite & Free-Threaded Python 3.14t](benchmarks-and-free-threaded.md) — `make bench`, `make bench-all`, `make bench-ft` (`.venv-ft` and `src/rs/target-ft`), `oha` harness (`tools/benchmarks/`), scheduled `.github/workflows/benchmark.yml`, `--group bench` dependency isolation, `tools/pgo/` Rust/CPython PGO tooling, and Criterion microbenchmarks.
