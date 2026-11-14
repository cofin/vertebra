---
type: Spec
flow_id: native-redis-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: 'Chapter 01: Standalone vertebra-redis Crate, [redis]/[valkey] Extras, Wheel Isolation, and Review Manifests'
description: Scaffold src/rs/vertebra-redis (vertebra._redis, gil_used = false) over pure-Rust redis 1.8.1 + rustls, wire [redis]/[valkey] extras and Makefile/release matrix, enforce companion wheel isolation across sibling extensions, and register review manifests.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T23:14:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:14:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T23:14:00Z'
  commit: 1b191388
tags:
- redis
- valkey
- rust
- wheel
- packaging
- tls
- review
sources:
- id: cargo-ws
  resource: src/rs/Cargo.toml
  title: Rust workspace member manifest
- id: mysql-cargo
  resource: src/rs/vertebra-mysql/Cargo.toml
  title: Pure-Rust async network PyO3 crate precedent
- id: pg-pyproj
  resource: src/rs/vertebra-postgres/pyproject.toml
  title: Maturin standalone wheel isolation precedent
- id: root-pyproject
  resource: pyproject.toml
  title: Root vertebra-core extras, uv sources, maturin excludes, and bumpversion
- id: makefile
  resource: Makefile
  title: Polyglot build, lint, stubtest, and wheel-check targets
- id: release-wf
  resource: .github/workflows/release.yaml
  title: Release wheel and sdist build matrix
- id: review-areas
  resource: .agents/skills/review-core/references/areas.toml
  title: Review router area ownership manifest
- id: review-checklist
  resource: .agents/skills/review-core/references/checklist.md
  title: Review checklist including FFI-09 stub list
- id: pyo3-skill
  resource: .agents/skills/pyo3-bridge/SKILL.md
  title: PyO3 native extension engineering skill
- id: wheel-tests
  resource: src/py/tests/unit/db/test_optional_driver_wheels.py
  title: Sibling PyO3 wheel isolation test gate
---

# Chapter 01: Standalone vertebra-redis Crate, [redis]/[valkey] Extras, Wheel Isolation, and Review Manifests

<!-- truth: start -->
- `src/rs/vertebra-redis` compiles `vertebra._redis` (`#[pymodule(gil_used = false)]`) over pure-Rust `redis = "=1.8.1"` + `rustls` (`tokio-rustls-comp`, `cluster-async`, `connection-manager`, `cache-aio`, `streams`, `script`, `acl`, `sentinel`) for Redis `7.2+`/`8.0` and Valkey `8.0+`/`9.0` [^mysql-cargo] [^pg-pyproj].
- `pyproject.toml` declares both `redis = ["vertebra-redis==0.3.0"]` and `valkey = ["vertebra-redis==0.3.0"]` extras plus `uv.sources`, `maturin.exclude`, and `bumpversion` (omitting `[project.optional-dependencies].all` per `test_optional_driver_wheels.py`) [^root-pyproject] [^wheel-tests].
- `Makefile` adds `redis-test` and `redis-wheel-check`, and `.github/workflows/release.yaml` builds wheels + sdist and verifies both `[redis]` and `[valkey]` extras [^makefile] [^release-wf].
- Sibling maturin wheels (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`, plus `sqlite` if present) exclude `"**/py/vertebra/_redis.pyi"`, enforced reciprocally in `test_optional_driver_wheels.py` [^pg-pyproj] [^wheel-tests].
- `areas.toml`, `checklist.md` (`FFI-09`), and `.agents/skills/pyo3-bridge/SKILL.md` register `vertebra-redis` and `_redis.pyi` [^review-areas] [^review-checklist] [^pyo3-skill].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`pyproject.toml` lacks `redis` and `valkey` optional extras while `SyncRedisDriver`/`AsyncRedisDriver` run over Python `socket` / `asyncio.open_connection` [^root-pyproject]. This chapter scaffolds the single pure-Rust `vertebra-redis` wheel backing both `vertebra-core[redis]` and `vertebra-core[valkey]`, establishes `_redis.pyi` + build/release targets, enforces reciprocal wheel exclusions across all sibling PyO3 manifests, and registers review routing [^mysql-cargo] [^pg-pyproj] [^review-areas].

**Non-Goals:** URL/TLS/GCP IAM config and error mapping (`ch02:t01`); client topologies, driver cutover, and notification split (`ch02`–`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Rust crate & workspace | `src/rs/vertebra-redis/{Cargo.toml,pyproject.toml,src/lib.rs}`, `src/rs/Cargo.toml`, `pyproject.toml` [^cargo-ws] [^mysql-cargo] [^root-pyproject] | `vertebra._redis` (`GIL_USED = False`, CRC16/hash_slot/redirect helpers), `[redis]` & `[valkey]` |
| Stubs & build matrix | `src/py/vertebra/_redis.pyi`, `Makefile`, `.github/workflows/release.yaml` [^makefile] [^release-wf] | `make redis-test`, `make redis-wheel-check`, wheel/sdist builds |
| Wheel isolation | `src/rs/vertebra-*/pyproject.toml`, `test_optional_driver_wheels.py` [^pg-pyproj] [^wheel-tests] | Disjoint `.pyi` payloads across every PyO3 crate |
| Review routing | `areas.toml`, `checklist.md`, `pyo3-bridge/SKILL.md` [^review-areas] [^review-checklist] [^pyo3-skill] | `FFI-09` stubtest gate and path ownership |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [ADR-004 Optional Driver Wheels and SHM Ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 / R2**: Scaffold `src/rs/vertebra-redis` (`Cargo.toml`, `pyproject.toml`, `src/lib.rs`) over `redis = "=1.8.1"` + `rustls` (`ring` crypto provider), exporting `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect`; register in `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and `pyproject.toml` (`[redis]` + `[valkey]` extras, `uv.sources`, `maturin.exclude`, `bumpversion`; omit `.all`) (`t01`).
- **R3**: Author `src/py/vertebra/_redis.pyi` and wire `Makefile` (`dev`, `build`, `lint`, `lint-all`, `py-test`, `redis-test`, `redis-wheel-check`) and `.github/workflows/release.yaml` (`t02`).
- **R5a**: Exclude `"**/py/vertebra/_redis.pyi"` across all sibling PyO3 `pyproject.toml` files (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`, plus `sqlite` if present) and assert `[redis]`/`[valkey]` wheel isolation in `test_optional_driver_wheels.py` (`t03`).
- **R4**: Register `src/rs/vertebra-redis/**` and `src/py/vertebra/_redis.pyi` in `areas.toml`, `checklist.md` (`FFI-09`), and `pyo3-bridge/SKILL.md` (`t04`).

```toml
# src/rs/vertebra-redis/Cargo.toml
[package]
name = "vertebra-redis"
version.workspace = true
edition.workspace = true

[lib]
name = "_redis"
crate-type = ["cdylib", "rlib"]

[dependencies]
bytes = { workspace = true }
futures-util = "0.3"
parking_lot = { workspace = true }
pyo3 = { workspace = true }
redis = { version = "=1.8.1", default-features = false, features = ["tokio-rustls-comp", "cluster-async", "connection-manager", "cache-aio", "streams", "script", "acl", "sentinel"] }
rustls = { workspace = true }
tokio = { workspace = true }
vertebra-core = { path = "../vertebra-core" }
vertebra-db = { path = "../vertebra-db", features = ["pyo3"] }

[build-dependencies]
pyo3-build-config = { workspace = true }
```

**Integration Flow:**
- Both `vertebra-core[redis]` and `vertebra-core[valkey]` install `vertebra-redis` (`vertebra._redis`).
- `make redis-wheel-check` and `pytest src/py/tests/unit/db/test_optional_driver_wheels.py` guarantee disjoint extension wheels.
- `make review-check` routes `src/rs/vertebra-redis/**` and `src/py/vertebra/_redis.pyi` to `pyo3-bridge`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| OpenSSL dependency | `default-features = false` + `tokio-rustls-comp` | Zero OpenSSL / C TLS linkage across Linux/macOS/Windows | Pure-Rust TLS |
| Sibling wheel overlap | Sibling wheel omits `_redis.pyi` exclusion | `test_optional_driver_wheels.py` fails immediately | Disjoint extension wheels |
| Stub drift | `mypy.stubtest vertebra._redis` | Fails build if Rust exports diverge from `_redis.pyi` | `FFI-09` parity |

## 3. Documentation & OKF Knowledge Plan

Public docs, `AGENTS.md`, `.agents/bundles/product/product.md`, and OKF chapters execute in `ch04:t03` and `ch04:t04`; review skill registration executes here in `t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Scaffold vertebra-redis Crate and Wire Cargo Workspace + [redis]/[valkey] Extras | `src/rs/vertebra-redis/{Cargo.toml,pyproject.toml,src/lib.rs}`, `src/rs/Cargo.{toml,lock}`, `pyproject.toml` | `cargo test -p vertebra-redis` |
| `t02` | Author _redis.pyi Stub, Makefile Targets, and Release Matrix | `src/py/vertebra/_redis.pyi`, `Makefile`, `.github/workflows/release.yaml` | `make dev && mypy.stubtest vertebra._redis` |
| `t03` | Enforce _redis Companion Wheel Exclusions Across All Sibling Extensions | `src/rs/vertebra-*/pyproject.toml` (sibling manifests) | `make redis-wheel-check`, `src/py/tests/unit/db/test_optional_driver_wheels.py` |
| `t04` | Register vertebra-redis in Review Manifests, Checklist, and Skills | `.agents/skills/review-core/references/areas.toml`, `.agents/skills/review-core/references/checklist.md`, `.agents/skills/pyo3-bridge/SKILL.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-crate-workspace-and-root-extras.md](tasks/t01-crate-workspace-and-root-extras.md) — Create `src/rs/vertebra-redis` with pure-Rust `redis 1.8.1` + `rustls` and `gil_used = false`, and add `vertebra-redis` to `src/rs/Cargo.{toml,lock}` and root `pyproject.toml` `[redis]`, `[valkey]`, `uv.sources`, `maturin.exclude`, and `bumpversion`.
- [ ] [t02-stub-makefile-and-release-matrix.md](tasks/t02-stub-makefile-and-release-matrix.md) — Create `src/py/vertebra/_redis.pyi` and wire `Makefile` `redis-test`/`redis-wheel-check` and `.github/workflows/release.yaml`.
- [ ] [t03-sibling-wheel-exclusions-and-isolation-gate.md](tasks/t03-sibling-wheel-exclusions-and-isolation-gate.md) — Exclude `_redis.pyi` from all sibling extension `pyproject.toml` manifests and register `redis`/`valkey` in `test_optional_driver_wheels.py`.
- [ ] [t04-review-manifest-checklist-and-skills.md](tasks/t04-review-manifest-checklist-and-skills.md) — Add `vertebra-redis` paths and `_redis.pyi` to `areas.toml`, `checklist.md` (`FFI-09`), and `pyo3-bridge/SKILL.md`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero sibling wheels include `_redis.pyi`; `vertebra-redis` links zero OpenSSL C libraries.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis && make redis-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q && make review-check`

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^mysql-cargo]: `src/rs/vertebra-mysql/Cargo.toml` — Pure-Rust async network PyO3 crate precedent.
[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
[^makefile]: `Makefile` — Polyglot build, lint, stubtest, and wheel-check targets.
[^release-wf]: `.github/workflows/release.yaml` — Release wheel and sdist build matrix.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review router area ownership manifest.
[^review-checklist]: `.agents/skills/review-core/references/checklist.md` — Review checklist including FFI-09 stub list.
[^pyo3-skill]: `.agents/skills/pyo3-bridge/SKILL.md` — PyO3 native extension engineering skill.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
