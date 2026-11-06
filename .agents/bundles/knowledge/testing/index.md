---
title: "Testing & Verification Index"
description: "Index of Vertebra's testing, verification, and benchmark chapters across Rust, Python, TypeScript, and free-threaded Python 3.14t/3.15t."
tags:
  - testing
  - index
  - verification
  - benchmarks
---

# Testing & Verification Index

- [Polyglot Test Matrix & Environment Setup](polyglot-test-matrix.md) — `make setup-env` (`tools/scripts/setup-env.sh`), `make dev` (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`), `make rs-test`, `make py-test`, `make ts-test`, `make lint-all`, `make check`, CI workflow matrix (`.github/workflows/ci.yml`: Python `3.12`–`3.15` and free-threaded `3.14t`/`3.15t`, Rust feature matrix), and `/dev/shm` test hygiene.
- [Benchmark Suite & Free-Threaded Python 3.14t/3.15t](benchmarks-and-free-threaded.md) — `make bench`, `make bench-all`, `make bench-ft` (`FT_PYTHON` `3.15t`/`3.14t`, `.venv-ft`, and `src/rs/target-ft`), `oha` harness (`tools/benchmarks/` across `vtb-embedded`, `vtb-shm`, `granian-rsgi`, `--jit`), scheduled `.github/workflows/benchmark.yml`, `--group bench` dependency isolation, `tools/pgo/` Rust/CPython PGO tooling, and Criterion microbenchmarks.
- [Agentic Review & Benchmark Gates](agentic-review.md) — Manifest-driven review system, static subagent tool validation, checklist and suppression workflow, and benchmark regression gates (`make review-check`, `make review-route`, `make rs-bench`, `make bench-micro`, `make bench-baseline`, `make bench-check`).
