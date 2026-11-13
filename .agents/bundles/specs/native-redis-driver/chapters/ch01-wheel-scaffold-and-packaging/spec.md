---
type: Spec
flow_id: native-redis-driver
chapter_id: ch01-wheel-scaffold-and-packaging
title: "Chapter 01: Standalone vertebra-redis Crate, [redis]/[valkey] Extras, Build Targets, and Review Manifests"
description: "Scaffold src/rs/vertebra-redis (vertebra._redis, gil_used = false) over pure-Rust redis 1.8.1 + rustls supporting Redis 7.2+/8.0 and Valkey 8.0+/9.0, wire [redis] and [valkey] extras, Makefile targets, release matrix, and review manifests."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - redis
  - valkey
  - rust
  - wheel
  - packaging
sources:
  - id: cargo-ws
    resource: src/rs/Cargo.toml
    title: "Rust workspace member manifest"
  - id: mysql-cargo
    resource: src/rs/vertebra-mysql/Cargo.toml
    title: "Pure-Rust async network PyO3 crate precedent"
  - id: pg-pyproj
    resource: src/rs/vertebra-postgres/pyproject.toml
    title: "Maturin standalone wheel isolation precedent"
  - id: root-pyproject
    resource: pyproject.toml
    title: "Root vertebra-core extras, uv sources, maturin excludes, and bumpversion"
  - id: makefile
    resource: Makefile
    title: "Polyglot build, lint, stubtest, and wheel-check targets"
  - id: release-wf
    resource: .github/workflows/release.yaml
    title: "Release wheel and sdist build matrix"
  - id: review-areas
    resource: .agents/skills/review-core/references/areas.toml
    title: "Review router area ownership manifest"
  - id: review-checklist
    resource: .agents/skills/review-core/references/checklist.md
    title: "Review checklist including FFI-09 stub list"
---

# Chapter 01: Standalone vertebra-redis Crate, [redis]/[valkey] Extras, Build Targets, and Review Manifests

<!-- truth: start -->
- `src/rs/vertebra-redis` compiles `vertebra._redis` (`#[pymodule(gil_used = false)]`) over pure-Rust `redis = "=1.8.1"` + `rustls` (`tokio-rustls-comp`, `cluster-async`, `connection-manager`, `cache-aio`, `streams`, `script`, `acl`, `sentinel`) for Redis `7.2+`/`8.0` and Valkey `8.0+`/`9.0` [^mysql-cargo] [^pg-pyproj].
- `pyproject.toml` declares both `redis = ["vertebra-redis==0.3.0"]` and `valkey = ["vertebra-redis==0.3.0"]` extras plus `all`, `uv.sources`, `maturin.exclude`, and `bumpversion` [^root-pyproject].
- `Makefile` adds `redis-test` and `redis-wheel-check`, and `.github/workflows/release.yaml` builds wheels + sdist and tests both `[redis]` and `[valkey]` extras [^makefile] [^release-wf].
- `areas.toml`, `checklist.md` (`FFI-09`), and `pyo3-bridge/SKILL.md` register `vertebra-redis` and `_redis.pyi` [^review-areas] [^review-checklist].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`pyproject.toml` currently lacks `redis` and `valkey` extras while `SyncRedisDriver`/`AsyncRedisDriver` run over Python `socket` / `asyncio.open_connection` [^root-pyproject]. This chapter scaffolds the shared pure-Rust `vertebra-redis` wheel serving both `vertebra-core[redis]` and `vertebra-core[valkey]`.

**Non-Goals:** Sibling wheel exclusions (`ch02`); Client topologies, driver cutover, and notification split (`ch03`–`ch06`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Rust crate | `src/rs/vertebra-redis/{Cargo.toml,pyproject.toml,src/lib.rs}` [^mysql-cargo] | `vertebra._redis` (`GIL_USED = False`, CRC16/redirect helpers) |
| Packaging | `src/rs/Cargo.toml`, `pyproject.toml`, `Makefile`, `release.yaml` [^cargo-ws] [^root-pyproject] [^makefile] [^release-wf] | `vertebra-core[redis]` & `vertebra-core[valkey]`, `make redis-test`, `make redis-wheel-check` |
| Review infra | `areas.toml`, `checklist.md`, `pyo3-bridge/SKILL.md` [^review-areas] [^review-checklist] | `FFI-09` stubtest gate and path ownership |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1**: Scaffold `src/rs/vertebra-redis` (`Cargo.toml`, `pyproject.toml`, `src/lib.rs`) over `redis = "=1.8.1"` + `rustls` (`ring` crypto provider), exporting `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect` (`t01`).
- **R2**: Add `vertebra-redis` to `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and `pyproject.toml` (`[redis]` + `[valkey]` + `[all]`, `uv.sources`, `maturin.exclude`, `bumpversion`) (`t02`).
- **R3**: Author `src/py/vertebra/_redis.pyi` and wire `Makefile` (`redis-test`, `redis-wheel-check`, `stubtest`) and `.github/workflows/release.yaml` (`t03`).
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
bytes.workspace = true
futures-util = "0.3"
parking_lot.workspace = true
pyo3.workspace = true
redis = { version = "=1.8.1", default-features = false, features = ["tokio-rustls-comp", "cluster-async", "connection-manager", "cache-aio", "streams", "script", "acl", "sentinel"] }
rustls.workspace = true
tokio.workspace = true
vertebra-core = { path = "../vertebra-core" }
vertebra-db = { path = "../vertebra-db", features = ["pyo3"] }
```

**Integration Flow:**
- Both `vertebra-core[redis]` and `vertebra-core[valkey]` install `vertebra-redis` (`vertebra._redis`).
- `make review-check` routes `src/rs/vertebra-redis/**` and `src/py/vertebra/_redis.pyi` to `pyo3-bridge` (`FFI`).

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| OpenSSL dependency | `default-features = false` + `tokio-rustls-comp` | Zero OpenSSL / C TLS linkage across Linux/macOS/Windows | Pure-Rust TLS |
| Stub drift | `mypy.stubtest vertebra._redis` | Fails build if Rust exports diverge from `_redis.pyi` | `FFI-09` parity |

## 3. Documentation & OKF Knowledge Plan

Public docs and OKF updates execute in `ch07:t03` and `ch08`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` | `t01` | `src/rs/vertebra-redis/{Cargo.toml,pyproject.toml,src/lib.rs}` | `cargo check -p vertebra-redis` |
| `R2` | `t02` | `src/rs/Cargo.{toml,lock}`, `pyproject.toml` | `cargo test -p vertebra-redis` |
| `R3` | `t03` | `src/py/vertebra/_redis.pyi`, `Makefile`, `.github/workflows/release.yaml` | `mypy.stubtest vertebra._redis` |
| `R4` | `t04` | `areas.toml`, `checklist.md`, `pyo3-bridge/SKILL.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-crate-and-pymodule-scaffold.md](tasks/t01-crate-and-pymodule-scaffold.md) — Create src/rs/vertebra-redis with pure-Rust redis 1.8.1 + rustls, gil_used = false, and CRC16/redirect helpers.
- [ ] [t02-workspace-and-root-extras.md](tasks/t02-workspace-and-root-extras.md) — Add vertebra-redis to Cargo workspace and root pyproject.toml [redis], [valkey], [all], uv.sources, maturin.exclude, and bumpversion.
- [ ] [t03-stub-makefile-and-release-matrix.md](tasks/t03-stub-makefile-and-release-matrix.md) — Create src/py/vertebra/_redis.pyi and wire Makefile redis-test/redis-wheel-check and .github/workflows/release.yaml.
- [ ] [t04-review-manifest-checklist-and-skills.md](tasks/t04-review-manifest-checklist-and-skills.md) — Add vertebra-redis paths and _redis.pyi to areas.toml, checklist.md (FFI-09), and pyo3-bridge/SKILL.md.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: `#[pymodule(gil_used = false)]` is declared from initial scaffold; `default-features = false` avoids OpenSSL.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis && make review-check`

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^mysql-cargo]: `src/rs/vertebra-mysql/Cargo.toml` — Pure-Rust async network PyO3 crate precedent.
[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
[^makefile]: `Makefile` — Polyglot build, lint, stubtest, and wheel-check targets.
[^release-wf]: `.github/workflows/release.yaml` — Release wheel and sdist build matrix.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review router area ownership manifest.
[^review-checklist]: `.agents/skills/review-core/references/checklist.md` — Review checklist including FFI-09 stub list.
