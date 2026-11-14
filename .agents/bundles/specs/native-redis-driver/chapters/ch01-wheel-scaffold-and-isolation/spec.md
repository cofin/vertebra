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
plan_commit: 104fd6fa
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 104fd6fa
tags:
- redis
- valkey
- rust
- wheel
- packaging
- tls
- gcp
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
- id: wheel-tests
  resource: src/py/tests/unit/db/test_optional_driver_wheels.py
  title: Sibling PyO3 wheel isolation test gate
- id: gcp-capsule
  resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
  title: C-ABI GCP IAM token cache capsule
- id: redis-cfg
  resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
  title: RedisConfig topology, tracking, and Memorystore IAM fields
---

# Chapter 01: Standalone vertebra-redis Crate, [redis]/[valkey] Extras, Wheel Isolation, and Review Manifests

<!-- truth: start -->
- `src/rs/vertebra-redis` compiles `vertebra._redis` (`#[pymodule(gil_used = false)]`) over pure-Rust `redis = "=1.8.1"` + `rustls` (`tokio-rustls-comp`, `cluster-async`, `connection-manager`, `cache-aio`, `streams`, `script`, `acl`, `sentinel`) for Redis `7.2+`/`8.0` and Valkey `8.0+`/`9.0` [^mysql-cargo] [^pg-pyproj].
- `pyproject.toml` declares both `redis = ["vertebra-redis==0.3.0"]` and `valkey = ["vertebra-redis==0.3.0"]` extras plus `uv.sources`, `maturin.exclude`, and `bumpversion` (without an `[all]` extra per `test_optional_driver_wheels.py`) [^root-pyproject].
- `Makefile` adds `redis-test` and `redis-wheel-check`, and `.github/workflows/release.yaml` builds wheels + sdist and tests both `[redis]` and `[valkey]` extras [^makefile] [^release-wf].
- `areas.toml`, `checklist.md` (`FFI-09`), and `pyo3-bridge/SKILL.md` register `vertebra-redis` and `_redis.pyi` [^review-areas] [^review-checklist].
- All sibling maturin wheels exclude `"**/py/vertebra/_redis.pyi"` (and reciprocal `_sqlite.pyi` if landed), verified by `test_optional_driver_wheels.py` [^pg-pyproj] [^wheel-tests].
- `config.rs` normalizes `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://` schemes for `redis 1.8.1` while rejecting `memory://` with `ImproperConfigurationError` [^redis-cfg].
- GCP Memorystore for Redis/Valkey IAM authentication refreshes tokens dynamically through `GcpTokenCacheCapsule` [^gcp-capsule] [^redis-cfg].
- `errors.rs` maps `redis::RedisError` (`NOAUTH`, `WRONGPASS`, `NOPERM`, `READONLY`, `BUSY`, `LOADING`, `CLUSTERDOWN`, `CROSSSLOT`, `NOSCRIPT`, `WRONGTYPE`, I/O timeouts) to `vertebra._vertebra.DatabaseError` subclasses with `is_transient` [^cargo-ws].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`pyproject.toml` currently lacks `redis` and `valkey` extras while `SyncRedisDriver`/`AsyncRedisDriver` run over Python `socket` / `asyncio.open_connection` [^root-pyproject]. This chapter scaffolds the shared pure-Rust `vertebra-redis` wheel serving both `vertebra-core[redis]` and `vertebra-core[valkey]`.

**Non-Goals:** Sibling wheel exclusions (`ch02`); Client topologies, driver cutover, and notification split (`ch02`–`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Rust crate | `src/rs/vertebra-redis/{Cargo.toml,pyproject.toml,src/lib.rs}` [^mysql-cargo] | `vertebra._redis` (`GIL_USED = False`, CRC16/redirect helpers) |
| Packaging | `src/rs/Cargo.toml`, `pyproject.toml`, `Makefile`, `release.yaml` [^cargo-ws] [^root-pyproject] [^makefile] [^release-wf] | `vertebra-core[redis]` & `vertebra-core[valkey]`, `make redis-test`, `make redis-wheel-check` |
| Review infra | `areas.toml`, `checklist.md`, `pyo3-bridge/SKILL.md` [^review-areas] [^review-checklist] | `FFI-09` stubtest gate and path ownership |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Project Workflow](../../../../knowledge/workflow.md).

Both Redis `7.2+`/`8.0` and Valkey `8.0+`/`9.0` speak RESP3 over TCP, `rustls` TLS (`rediss://` / `valkeys://`), and Unix domain sockets (`unix://`), while GCP Memorystore uses short-lived OAuth2 IAM access tokens [^gcp-capsule] [^redis-cfg]. This chapter enforces wheel isolation and builds the URL, TLS, IAM, and error foundations of `vertebra-redis`.

**Non-Goals:** Command execution, client tracking, Cluster/Sentinel, Streams, and Pub/Sub (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Wheel isolation | `src/rs/vertebra-*/pyproject.toml`, `test_optional_driver_wheels.py` [^wheel-tests] | Disjoint `.pyi` wheel payloads |
| Config, IAM & errors | `src/rs/vertebra-redis/src/{config,errors,lib}.rs` [^gcp-capsule] [^redis-cfg] | `valkey://`/`valkeys://`/`unix://` + `GcpTokenCacheCapsule` + `DatabaseError` |

**Knowledge Anchors:** [ADR-004 Optional Driver Wheels and SHM Ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1**: Scaffold `src/rs/vertebra-redis` (`Cargo.toml`, `pyproject.toml`, `src/lib.rs`) over `redis = "=1.8.1"` + `rustls` (`ring` crypto provider), exporting `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect` (`t01`).
- **R2**: Add `vertebra-redis` to `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and `pyproject.toml` (`[redis]` + `[valkey]` extras, `uv.sources`, `maturin.exclude`, `bumpversion`) (`t02`).
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
vertebra-core = { workspace = true }
vertebra-db = { workspace = true, features = ["pyo3"] }

[build-dependencies]
pyo3-build-config.workspace = true
```

**Integration Flow:**
- Both `vertebra-core[redis]` and `vertebra-core[valkey]` install `vertebra-redis` (`vertebra._redis`).
- `make review-check` routes `src/rs/vertebra-redis/**` and `src/py/vertebra/_redis.pyi` to `pyo3-bridge` (`FFI`).

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| OpenSSL dependency | `default-features = false` + `tokio-rustls-comp` | Zero OpenSSL / C TLS linkage across Linux/macOS/Windows | Pure-Rust TLS |
| Stub drift | `mypy.stubtest vertebra._redis` | Fails build if Rust exports diverge from `_redis.pyi` | `FFI-09` parity |

- **R5a**: Exclude `_redis.pyi` across sibling PyO3 `pyproject.toml` files and assert `[redis]`/`[valkey]` wheel isolation in `test_optional_driver_wheels.py` (`t01`–`t03`).
- **R5b**: Implement `RedisClientConfig` (`redis://`, `rediss://`, `valkey://`, `valkeys://`, `unix://`, `memory://` rejection, `GcpTokenCacheCapsule` IAM hook) and `RedisError` -> `DatabaseError` mapping (`t04`).

```rust
pub enum RedisTopology {
    Standalone,
    Cluster { nodes: Vec<(String, u16)>, max_redirections: usize },
    Sentinel { master_name: String, nodes: Vec<(String, u16)> },
}
// URL scheme normalization:
// valkey://host:port/db  -> redis://host:port/db
// valkeys://host:port/db -> rediss://host:port/db (rustls + ring)
// unix:///path/to.sock   -> redis+unix:///path/to.sock
// memory://...           -> ImproperConfigurationError
```

**Integration Flow:**
- `make redis-wheel-check` verifies `vertebra-redis` wheel isolation.
- `client_core.rs` (`ch03`) consumes `RedisClientConfig`, `GcpTokenCacheCapsule`, and `map_redis_error`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| `memory://` URL | `SyncRedisClient("memory://")` | Raises `ImproperConfigurationError` immediately | Fail-loud baseline preserved |
| `CROSSSLOT` / `WRONGTYPE` | Server error prefix `CROSSSLOT` or `WRONGTYPE` | `ProgrammingError` (`is_transient=False`) | Typed command error |
| `BUSY` / `CLUSTERDOWN` / I/O | Transient network or failover error | `OperationalError` (`is_transient=True`) | Retryable classification |

## 3. Documentation & OKF Knowledge Plan

Public docs and OKF updates execute in `ch04:t03` and `ch04:t04`.

URL schemes and Memorystore IAM are documented in `ch04:t03` and `ch04:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Scaffold vertebra-redis Crate and Wire Cargo Workspace + [redis]/[valkey] Extras | `src/rs/vertebra-redis/Cargo.toml`, `src/rs/vertebra-redis/pyproject.toml`, `src/rs/vertebra-redis/src/lib.rs` (+3 more) | `make review-check` |
| `t02` | Author _redis.pyi Stub, Makefile Targets, and Release Matrix | `src/py/vertebra/_redis.pyi`, `Makefile`, `.github/workflows/release.yaml` | `make review-check` |
| `t03` | Enforce _redis Companion Wheel Exclusions Across All Sibling Extensions | `src/rs/vertebra-bigquery/pyproject.toml`, `src/rs/vertebra-bundler/pyproject.toml`, `src/rs/vertebra-duckdb/pyproject.toml` (+6 more) | `src/py/tests/unit/db/test_optional_driver_wheels.py` |
| `t04` | Register vertebra-redis in Review Manifests, Checklist, and Skills | `.agents/skills/review-core/references/areas.toml`, `.agents/skills/review-core/references/checklist.md`, `.agents/skills/pyo3-bridge/SKILL.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-crate-workspace-and-root-extras.md](tasks/t01-crate-workspace-and-root-extras.md) — Create src/rs/vertebra-redis with pure-Rust redis 1.8.1 + rustls and gil_used = false, and add vertebra-redis to Cargo workspace and root pyproject.toml [redis], [valkey], uv.sources, maturin.exclude, and bumpversion.
- [ ] [t02-stub-makefile-and-release-matrix.md](tasks/t02-stub-makefile-and-release-matrix.md) — Create src/py/vertebra/_redis.pyi and wire Makefile redis-test/redis-wheel-check and .github/workflows/release.yaml.
- [ ] [t03-sibling-wheel-exclusions-and-isolation-gate.md](tasks/t03-sibling-wheel-exclusions-and-isolation-gate.md) — Exclude _redis* and vertebra-redis from all sibling extension pyproject.toml manifests and register redis/valkey in test_optional_driver_wheels.py.
- [ ] [t04-review-manifest-checklist-and-skills.md](tasks/t04-review-manifest-checklist-and-skills.md) — Add vertebra-redis paths and _redis.pyi to areas.toml, checklist.md (FFI-09), and pyo3-bridge/SKILL.md.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero sibling wheels include `_redis.pyi`; `memory://` is rejected without opening sockets.
- **Fast Chapter Gate (`< 60s`)**: `make redis-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-redis`

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^mysql-cargo]: `src/rs/vertebra-mysql/Cargo.toml` — Pure-Rust async network PyO3 crate precedent.
[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
[^makefile]: `Makefile` — Polyglot build, lint, stubtest, and wheel-check targets.
[^release-wf]: `.github/workflows/release.yaml` — Release wheel and sdist build matrix.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review router area ownership manifest.
[^review-checklist]: `.agents/skills/review-core/references/checklist.md` — Review checklist including FFI-09 stub list.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — C-ABI GCP IAM token cache capsule.
[^redis-cfg]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — RedisConfig topology, tracking, and Memorystore IAM fields.
