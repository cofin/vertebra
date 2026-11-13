---
type: Task
id: "native-postgres-driver:ch07-wheel-ci-and-benchmarks:t03"
flow_id: native-postgres-driver
chapter_id: ch07-wheel-ci-and-benchmarks
title: "Add Criterion postgres_native.rs benches and Makefile postgres targets"
description: "Implement Criterion codec/wire/pipeline/arrow microbenchmarks in vertebra-postgres and wire postgres-test/wheel-check/bench targets in Makefile."
status: stable
state: completed
commit: 9a3631bb
last_verified_at: "2026-11-13T22:11:30Z"
last_verified_commit: 9a3631bb
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on: 
  - "native-postgres-driver:ch07:t02"
files: 
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/vertebra-postgres/benches/postgres_native.rs
  - Makefile
tests: 
  - src/py/tests/integration/db/test_postgres_benchmarks.py
read_set:
  - src/rs/vertebra-postgres/Cargo.toml
  - Makefile
  - tools/benchmarks/postgres.py
estimated_loc: 235
tags:
  - rust
  - postgres
  - benchmarks
sources:
  - id: pg-cargo
    resource: src/rs/vertebra-postgres/Cargo.toml
    title: "vertebra-postgres Cargo manifest"
  - id: makefile
    resource: Makefile
    title: "Canonical Make targets"
---

# Task `t03`: Add Criterion postgres_native.rs benches and Makefile postgres targets

## Objective & Scope Boundary

Add `benches/postgres_native.rs`, `@pytest.mark.postgres_benchmark` orchestrator, and `Makefile` `postgres-{test,wheel-check,bench,bench-check}` targets [^pg-cargo] [^makefile].

## Bounded Read Manifest

- `vertebra-postgres/Cargo.toml`, `Makefile`, and `tools/benchmarks/postgres.py`.

## Contracts & Edge Cases

- Benchmark Protocol `3.0` frame decode, scalar/numeric/`pgvector` codecs, `PGCOPY`, and `PgOutputDecoder` without live sockets in Criterion.

## Test Matrix

- `cargo check -p vertebra-postgres --benches --features gcp` + offline collection of `test_postgres_benchmarks.py`.

## Verification

```bash
cargo check --manifest-path src/rs/Cargo.toml -p vertebra-postgres --benches --features gcp && uv run --no-sync pytest src/py/tests/integration/db/test_postgres_benchmarks.py -q
```

## Acceptance Criteria

- [x] Criterion benchmark target compiles with zero warnings and `Makefile` targets are wired.
- [x] Zero regressions in `ch07-wheel-ci-and-benchmarks` fast verification gate.

[^pg-cargo]: `src/rs/vertebra-postgres/Cargo.toml` — vertebra-postgres Cargo manifest.
[^makefile]: `Makefile` — Canonical Make targets.
