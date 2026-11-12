---
type: Task
id: "broker-pool-sharing:ch03:t01"
chapter_id: ch03-engine-registry-capsule
title: Capsule contract and _vertebra registry
description: Replace the static BROKER_ENGINE_BACKENDS map with BrokerPoolBackend, BrokerEngineCapsule, and from_backend shims in vertebra-db, add BrokerEngineRegistry in vertebra._vertebra, and drop the dead MySQL static registration.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch02:t03"
read_set:
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/rs/vertebra-mysql/src/lib.rs
files:
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/rs/vertebra-mysql/src/lib.rs
tests:
  - src/rs/vertebra-db/tests/broker_engine_capsule_tests.rs
  - src/py/tests/unit/extensions/test_shm_broker.py
estimated_loc: 250
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
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
    title: Existing trait and static map
  - id: gcp-capsule
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: Capsule precedent
  - id: lease-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: _vertebra broker bindings
tags:
  - capsule
  - ffi
---

# Task t01: Capsule Contract and `_vertebra` Registry

## Objective & Scope Boundary

1. In `src/rs/vertebra-db/src/lib.rs` [^db-registry]: keep `BrokerEngineBackend` (identity and `normalize_statement`) and add `pub trait BrokerPoolBackend: BrokerEngineBackend` with `open_pool`, `execute`, `close_pool`; delete `BROKER_ENGINE_BACKENDS`, `register_broker_engine_backend`, and `lookup_broker_engine_backend`; add `BROKER_ENGINE_CAPSULE_NAME`, `BROKER_ENGINE_ABI_VERSION`, `BrokerBytesVisitFn`, `BrokerEngineCapsule`, `BrokerEngineCapsule::from_backend(Arc<dyn BrokerPoolBackend>)` (boxes the `Arc` behind `ctx`; every shim wraps the call in `std::panic::catch_unwind`), and `new_broker_engine_pycapsule` with a destructor that calls `drop_ctx_fn`, following `py_async.rs` [^gcp-capsule].
2. In `src/rs/vertebra-py/src/broker.rs` [^lease-broker]: add `#[pyclass] BrokerEngineRegistry` with static methods backed by `static ENGINES: LazyLock<DashMap<u16, ForeignEngineHandle>>` local to `_vertebra`; `ForeignEngineHandle` owns a cloned ctx and calls `drop_ctx_fn` on drop; `register` validates the capsule name with `PyCapsule_IsValid` and checks `abi_version`.
3. In `src/rs/vertebra-mysql/src/lib.rs`: delete `register_mysql_broker_backend` and its call in `_mysql`, because the static map no longer exists. `MySQLBrokerBackend` keeps its `BrokerEngineBackend` impl; `ch04 / t01` adds `BrokerPoolBackend` and the exported capsule. The workspace compiles after this task.
4. Stubs for `BrokerEngineRegistry` and the other new `_vertebra` symbols land in `t02` together with their first Python caller.

## Bounded Read Manifest

- `src/rs/vertebra-db/src/lib.rs`, `src/rs/vertebra-core/src/gcp_auth.rs` (`GcpTokenCacheCapsule`, `SharedGcpTokenCacheHandle`), `src/rs/vertebra-db/src/py_async.rs` (capsule destructor), `src/rs/vertebra-py/src/broker.rs`, `src/rs/vertebra-mysql/src/lib.rs` (`MySQLBrokerBackend`, `register_mysql_broker_backend`).

## Symbol & Signature Contracts

- Chapter §2.2 verbatim. Return codes: `0` ok, `1` engine error (message delivered through the visitor), `2` panic.
- `BrokerEngineRegistry.execute` releases the GIL (`py.detach`) around `execute_fn`.

## Edge Cases & Error Semantics

- Chapter §2.4 matrix applies in full.
- Null `ctx` in capsule: `RuntimeError("null broker engine capsule context")`.
- Unregistered tag in `execute`/`normalize`: `KeyError(f"no broker engine registered for dialect tag {tag}")`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-db/tests/broker_engine_capsule_tests.rs` (new integration test file for the new capsule module), `src/py/tests/unit/extensions/test_shm_broker.py`. Dependency category: in-process FFI.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Rust shim normalize | `from_backend(Arc::new(EchoBackend))`; call `normalize_fn` with `"SELECT 1"` | Visitor receives `"SELECT 1"`; returns param count `0` |
| Rust shim panic | Backend whose `execute` panics | `execute_fn` returns `2` |
| Clone/drop balance | Clone ctx twice, drop three times | `Arc::strong_count` returns to `0` (observed via `Weak::upgrade() == None`) |
| Python wrong object | `BrokerEngineRegistry.register(object())` | `TypeError` |
| Python unknown tag | `BrokerEngineRegistry.normalize(99, "x")` | `KeyError` |

## Verification

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features tls,compression,gcp --test broker_engine_capsule_tests`.
- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k registry`.
- `cargo clippy --manifest-path src/rs/Cargo.toml --workspace --features tls,compression,gcp -- -D warnings`.

## Acceptance Criteria

- [ ] The static per-cdylib map is gone; `_vertebra` owns the only registry (R8).
- [ ] Capsule ABI is versioned and panic-safe (R8).

## Notes & Discoveries

- [2026-11-12 01:40] `lookup_broker_engine_backend` has no callers at HEAD; only `vertebra-mysql` registers.
- [2026-11-12 01:40] Splitting pool methods into `BrokerPoolBackend` keeps `MySQLBrokerBackend` compiling until `ch04 / t01` adds pool support.

[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Trait and static map.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — Capsule precedent.
[^lease-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — Broker bindings.
