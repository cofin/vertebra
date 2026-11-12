---
type: Task
id: "broker-pool-sharing:ch08:t01"
chapter_id: ch08-reclamation-and-acceptance
title: Export the PostgreSQL broker engine capsule
description: Add broker_engine_capsule() to the vertebra._postgres PyO3 module returning a BrokerEngineCapsule with dialect_tag DIALECT_TAG_POSTGRES, add its stub, and cover discovery and registration.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch03:t01"
  - "broker-pool-sharing:ch03:t02"
  - "broker-pool-sharing:ch04:t01"
depends_on_flows:
  - native-postgres-driver
read_set:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-mysql/src/lib.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/tests/unit/extensions/test_shm_broker.py
  - .agents/bundles/specs/broker-pool-sharing/chapters/ch08-reclamation-and-acceptance/spec.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/tests/unit/extensions/test_shm_broker.py
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
estimated_loc: 160
plan_revision: 3
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: pg-module
    resource: src/rs/vertebra-postgres/src/lib.rs::_postgres
    title: PostgreSQL PyO3 module entry point
  - id: capsule
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineCapsule
    title: C-ABI broker engine capsule contract (ch03 t01)
  - id: mysql-capsule
    resource: src/rs/vertebra-mysql/src/lib.rs::broker_engine_capsule
    title: MySQL capsule export precedent (ch04 t01)
tags:
  - postgres
  - capsule
  - broker
---

# Task t01: Export the PostgreSQL Broker Engine Capsule

## Objective & Scope Boundary

1. In `src/rs/vertebra-postgres/src/lib.rs`, add `#[pyfunction] fn broker_engine_capsule(py) -> PyResult<Bound<PyCapsule>>` registered in `_postgres` [^pg-module]. It builds `BrokerEngineCapsule { abi_version: BROKER_ENGINE_ABI_VERSION, dialect_tag: DIALECT_TAG_POSTGRES as u32, .. }` [^capsule] with `normalize_fn`, `open_pool_fn`, `execute_fn`, `close_pool_fn`, `drop_ctx_fn` backed by the permanent native PostgreSQL pool from `native-postgres-driver`, mirroring the MySQL export from `ch04 / t01` [^mysql-capsule]. Capsule name `BROKER_ENGINE_CAPSULE_NAME`.
2. `src/py/vertebra/_postgres.pyi`: `def broker_engine_capsule() -> object: ...`.
3. Tests in `test_shm_broker.py`.

Out of scope: registry and discovery (`ch03`), other engines (`ch04`).

## Bounded Read Manifest

- `vertebra-postgres/src/lib.rs` — `_postgres` and pool types [^pg-module].
- `vertebra-db/src/lib.rs` — `BrokerEngineCapsule`, `BROKER_ENGINE_CAPSULE_NAME`, `BROKER_ENGINE_ABI_VERSION` [^capsule].
- `vertebra-mysql/src/lib.rs` — `broker_engine_capsule` precedent [^mysql-capsule].
- `_postgres.pyi`; `test_shm_broker.py` — discovery tests from `ch03 / t02`.

## Symbol & Signature Contracts

- `vertebra._postgres.broker_engine_capsule() -> object` (a `PyCapsule` named `vertebra.BrokerEngineCapsule`).
- `BrokerEngineRegistry.register(vertebra._postgres.broker_engine_capsule()) == 1`.
- `BrokerEngineRegistry.normalize(1, "select $1")` returns PostgreSQL-normalized SQL and its stable hash.

## Edge Cases & Error Semantics

- Calling `broker_engine_capsule()` twice returns independent capsules; registering both is idempotent for tag `1`.
- Capsule destructor runs `drop_ctx_fn` exactly once.
- `vertebra._postgres` missing → discovery skips (covered by `ch03 / t02`).

## Public-Seam Test Blueprint

| Test | Input | Expected |
| --- | --- | --- |
| `test_engine_capsule_registers[postgres]` | `BrokerEngineRegistry.register(_postgres.broker_engine_capsule())` | returns `1`; `1 in registered_tags()` |
| `test_engine_capsule_normalize[postgres]` | `normalize(1, "SELECT  $1")` | normalized SQL string, `int` hash equal across two calls |
| `test_engine_capsule_double_register[postgres]` | register twice | `registered_tags().count(1) == 1` |

Parametrize into the existing per-engine capsule test with `pytest.importorskip("vertebra._postgres")` inside the fixture parameter.

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q`
- `cargo test -p vertebra-postgres && uv run --no-sync python -m mypy.stubtest vertebra._postgres`

## Acceptance Criteria

- Registry reports tag `1` after discovery when the PostgreSQL wheel is installed; matrix tests pass.

## Notes & Discoveries

- Blocked until `native-postgres-driver` lands its permanent pool type; the capsule wraps that pool, not a temporary one.
