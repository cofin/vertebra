---
type: Spec
flow_id: native-postgres-driver
chapter_id: ch07-wheel-ci-and-benchmarks
title: "Chapter 07: Wheel Isolation, CI Lanes, Free-Threaded Proofs, and Benchmarks"
description: "Add verify_postgres_wheel.py, CI/release 14-class smoke checks, 3.14t/3.15t concurrent pool tests, postgres-bench comparator, Criterion benches, and testing OKF sync."
status: stable
state: active
updated_at: "2026-11-13T21:59:10Z"
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-13T00:25:00Z"
generated:
  by: flow-plan
  at: "2026-11-13T00:25:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-13T00:25:00Z"
    commit: 18baadcd
tags:
  - postgres
  - wheel
  - ci
  - benchmarks
  - free-threaded
sources:
  - id: pg-cargo
    resource: src/rs/vertebra-postgres/Cargo.toml
    title: "vertebra-postgres crate manifest"
  - id: pyproject
    resource: pyproject.toml
    title: "Python extras and dependency groups"
  - id: makefile
    resource: Makefile
    title: "Build, test, wheel-check, and benchmark targets"
  - id: ci-yml
    resource: .github/workflows/ci.yml
    title: "GitHub Actions CI workflow"
  - id: release-yml
    resource: .github/workflows/release.yaml
    title: "Release wheel build and smoke matrix"
---

# Chapter 07: Wheel Isolation, CI Lanes, Free-Threaded Proofs, and Benchmarks

<!-- truth: start -->
- `verify_postgres_wheel.py` runs two isolated venv probes (without `[postgres]` -> `MissingDependencyError`; with wheel -> 14-class import + zero `_vertebra` leakage), wired into `Makefile`, `ci.yml`, and `release.yaml` [^makefile] [^ci-yml] [^release-yml].
- `[dependency-groups].postgres-bench`, `tools/benchmarks/postgres.py`, and Criterion `benches/postgres_native.rs` benchmark `vertebra._postgres` against `psycopg` and `asyncpg` without adding third-party drivers to runtime extras [^pg-cargo] [^pyproject].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Completes wheel packaging isolation, free-threaded 8-thread pool proofs, CI conformance lanes, and Rust/Python PostgreSQL benchmark harnesses.

## 2. Requirements, Concrete Contracts & Architecture

- `tools/scripts/verify_postgres_wheel.py`: verifies sdist/wheel file isolation, `MissingDependencyError` when uninstalled, and all 14 `_postgres` classes when installed, alongside aligning `src/rs/vertebra-postgres/pyproject.toml` and `test_packaging_and_pgo.py::test_optional_extension_pyproject_isolation_and_bundler_extra` to include `("postgres", "vertebra._postgres")` and `"postgres"` in `other_stubs`.
- `tools/benchmarks/postgres.py` + `benches/postgres_native.rs` + `Makefile`: `postgres-test`, `postgres-wheel-check`, `postgres-bench`, and offline-testable `postgres-bench-check`.

## 3. Documentation & OKF Knowledge Plan

`t04` updates `benchmarks-and-free-threaded.md`, `polyglot-test-matrix.md` (`243L -> <= 200L`), and `workflow.md` (`<= 200L`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R4` Wheel verifier, CI/release & free-threaded pool proof | `t01` | `verify_postgres_wheel.py`, `ci.yml`, `release.yaml` | `pytest test_freethreaded_gil.py` |
| `R4` `postgres-bench` group & comparator CLI | `t02` | `tools/benchmarks/postgres.py`, `pyproject.toml`, `uv.lock` | `pytest test_postgres_benchmarks.py` |
| `R4` Criterion `postgres_native.rs` & `Makefile` targets | `t03` | `Cargo.toml`, `benches/postgres_native.rs`, `Makefile` | `cargo check -p vertebra-postgres --benches` |
| `R5` Testing & workflow OKF sync (`<= 200L`) | `t04` | 3 OKF chapters | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-verify-postgres-wheel-ci-and-freethreaded.md](tasks/t01-verify-postgres-wheel-ci-and-freethreaded.md) — Implement two-probe verify_postgres_wheel.py, 14-class release smoke checks, zero-skip CI PG14-18 lanes, and 8-thread free-threaded pool tests.
- [ ] [t02-postgres-bench-group-and-comparator.md](tasks/t02-postgres-bench-group-and-comparator.md) — Declare [dependency-groups].postgres-bench and implement schema_version=1 PostgreSQL benchmark capture and threshold comparator CLI.
- [ ] [t03-criterion-benches-and-makefile-lanes.md](tasks/t03-criterion-benches-and-makefile-lanes.md) — Implement Criterion codec/wire/pipeline/arrow microbenchmarks in vertebra-postgres and wire postgres-test/wheel-check/bench targets in Makefile.
- [ ] [t04-testing-and-workflow-okf-sync.md](tasks/t04-testing-and-workflow-okf-sync.md) — Update benchmarks-and-free-threaded.md, trim polyglot-test-matrix.md (<= 200L), and add postgres-bench targets to workflow.md.

## 6. Current State & Remaining Plan Snapshot

Planned (`4` tasks). Fast `< 45s` offline gate; wheel build and live benchmark runs execute in `/flow:finish`.

```bash
uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py src/py/tests/unit/tools/test_postgres_benchmarks.py -q && cargo check --manifest-path src/rs/Cargo.toml -p vertebra-postgres --benches --features gcp && make review-check
```

[^pg-cargo]: `src/rs/vertebra-postgres/Cargo.toml` — vertebra-postgres crate manifest.
[^pyproject]: `pyproject.toml` — Python extras and dependency groups.
[^makefile]: `Makefile` — Build, test, wheel-check, and benchmark targets.
[^ci-yml]: `.github/workflows/ci.yml` — GitHub Actions CI workflow.
[^release-yml]: `.github/workflows/release.yaml` — Release wheel build and smoke matrix.
