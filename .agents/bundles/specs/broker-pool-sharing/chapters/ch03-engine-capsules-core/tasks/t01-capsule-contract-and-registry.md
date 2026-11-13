---
type: Task
id: "broker-pool-sharing:ch03:t01"
flow_id: broker-pool-sharing
chapter_id: ch03-engine-capsules-core
title: "C-ABI BrokerEngineCapsule Contract and _vertebra BrokerEngineRegistry"
description: "Replace static BROKER_ENGINE_BACKENDS in vertebra-db with BrokerEngineCapsule and _vertebra.BrokerEngineRegistry."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch02:t03"
files:
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/rs/vertebra-db/tests/broker_engine_capsule_tests.rs
read_set:
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/_vertebra.pyi
estimated_loc: 245
tags:
  - capsule
  - ffi
  - pyo3
sources:
  - id: db-registry
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend
    title: "Per-cdylib static BrokerEngineBackend registry in vertebra-db"
  - id: gcp-capsule
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: "Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs"
  - id: py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: "PyO3 LeaseBroker and StatementCache bindings"
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Type stubs for vertebra._vertebra"
---

# Task `t01`: C-ABI BrokerEngineCapsule Contract and _vertebra BrokerEngineRegistry

## Objective & Scope Boundary

Implement `#[repr(C)] BrokerEngineCapsule`, `BrokerPoolBackend`, `new_broker_engine_pycapsule`, and `BrokerEngineRegistry` per [`../spec.md` §2](../spec.md) [^db-registry] [^gcp-capsule] [^py-broker] [^vertebra-pyi]. Out of scope: engine-specific pool backends (`t03`–`t04`, `ch04`).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/lib.rs` — Current `BrokerEngineBackend` and `BROKER_ENGINE_BACKENDS` static map.
- `src/rs/vertebra-core/src/gcp_auth.rs` — `GcpTokenCacheCapsule` C-ABI vtable and refcount pattern.
- `src/rs/vertebra-db/src/py_async.rs` — `PyCapsule` naming and destructor safety.
- `src/rs/vertebra-py/src/broker.rs` — `_vertebra` broker bindings.
- `src/py/vertebra/_vertebra.pyi` — Type stubs for `_vertebra`.

## Contracts & Edge Cases

- Delete `BROKER_ENGINE_BACKENDS`, `register_broker_engine_backend`, and `lookup_broker_engine_backend` from `vertebra-db/src/lib.rs`.
- Wrap every `extern "C"` vtable trampoline in `std::panic::catch_unwind`, validate `abi_version == 1`, and add `// SAFETY:` comments (`RT-01`).
- **Edge Case**: Re-registering an existing `dialect_tag` atomically swaps the handle and invokes `drop_ctx_fn` once on the replaced handle.
- **Edge Case**: Passing a non-capsule or wrong-named `PyCapsule` raises `TypeError`.

## Test Matrix

- Rust tests verify `clone_ctx_fn`/`drop_ctx_fn` reference balance, `abi_version` mismatch rejection, and `catch_unwind` panic containment.
- `mypy.stubtest vertebra._vertebra` passes.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-py --features tls,compression,gcp
```

## Acceptance Criteria

- [ ] Static `BROKER_ENGINE_BACKENDS` is removed from `vertebra-db`.
- [ ] `BrokerEngineRegistry` safely registers, invokes, replaces, and drops C-ABI capsules.
- [ ] Zero regressions in the chapter fast verification gate.

[^db-registry]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineBackend` — Per-cdylib static BrokerEngineBackend registry in vertebra-db.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — Versioned C-ABI vtable PyCapsule precedent across PyO3 cdylibs.
[^py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker and StatementCache bindings.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Type stubs for vertebra._vertebra.
