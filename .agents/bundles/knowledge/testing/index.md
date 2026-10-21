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

- [Polyglot Test Matrix & Environment Setup](polyglot-test-matrix.md) — `make setup-env` (`tools/scripts/setup-env.sh`), `make rs-test`, `make py-test`, `make ts-test`, `make lint-all`, `make check`, maturin extension rebuilds, and `/dev/shm` test hygiene.
- [Benchmark Suite & Free-Threaded Python 3.14t](benchmarks-and-free-threaded.md) — `make bench`, `make bench-all`, `make bench-ft` (`.venv-ft` and `src/rs/target-ft`), `--group bench` dependency isolation, and Criterion microbenchmarks.
