---
type: Task
id: "broker-pool-sharing:ch04:t02"
chapter_id: ch04-engine-backend-registration
title: Oracle broker capsule
description: Implement BrokerPoolBackend for OracleBrokerBackend over the native Oracle client and export vertebra._oracle.broker_engine_capsule().
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch03:t02"
read_set:
  - src/rs/vertebra-oracle/src/lib.rs
  - src/rs/vertebra-oracle/src/pool.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/_oracle.pyi
  - src/py/tests/unit/extensions/test_shm_broker.py
files:
  - src/rs/vertebra-oracle/src/lib.rs
  - src/py/vertebra/_oracle.pyi
  - src/py/tests/unit/extensions/test_shm_broker.py
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
estimated_loc: 220
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
  - id: engine
    resource: src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool
    title: Oracle native normalization or pool surface
  - id: capsule
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineCapsule
    title: Capsule contract from ch03
tags:
  - engines
  - oracle
---

# Task t02: Oracle Broker Capsule

## Objective & Scope Boundary

1. In `src/rs/vertebra-oracle/src/lib.rs`, implement `OracleBrokerBackend` with `BrokerEngineBackend` (tag `DIALECT_TAG_ORACLE = 4`, name `"oracle"`, normalization via Oracle named binds pass through unchanged; `param_count` counts distinct `:name` tokens outside quotes and comments) and `BrokerPoolBackend` [^engine] [^capsule]: `open_pool` parses the DSN with the crate's existing config parser and creates `pool.rs::SyncOraclePool`; `execute` decodes the `dbrpc` header (`vertebra_core::dbrpc::decode_header`) and msgspec-compatible body, runs the operation, and encodes a `DbRpcResponse`; `close_pool` drains and closes connections.
2. Add `#[pyfunction] broker_engine_capsule(py) -> PyResult<Bound<PyCapsule>>` calling `vertebra_db::new_broker_engine_pycapsule(py, Arc::new(OracleBrokerBackend::default()))` and register it in `_oracle`.
3. Add `def broker_engine_capsule() -> object: ...` to `src/py/vertebra/_oracle.pyi`.
4. Add the canonical tests below to `test_shm_broker.py`.

Out of scope: broker host dispatch (`ch05 / t03`), quarantine (`ch05 / t01`).

## Bounded Read Manifest

- `src/rs/vertebra-oracle/src/lib.rs` — pymodule `_oracle` and native client types.
- `src/rs/vertebra-oracle/src/pool.rs` — normalization or pool surface [^engine].
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend`, `new_broker_engine_pycapsule` [^capsule].
- `src/py/vertebra/_oracle.pyi`, `src/py/tests/unit/extensions/test_shm_broker.py`.

## Symbol & Signature Contracts

- `#[derive(Default)] pub struct OracleBrokerBackend { pools: DashMap<u64, ...>, next_id: AtomicU64 }`.
- `fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String>`; ids start at `1`.
- `fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String>` supports every `DbRpcOp`; `Savepoint` uses the engine's savepoint SQL; `Cancel` uses the engine's native cancel.
- Python: `vertebra._oracle.broker_engine_capsule() -> object` (a `PyCapsule` named `vertebra.BrokerEngineCapsule`).

## Edge Cases & Error Semantics

- Invalid DSN: `Err("invalid oracle DSN: <reason>")`.
- Unknown pool id: `Err("unknown pool id <id>")`.
- `max_size = 0`: `Err("max_size must be >= 1")`.
- Driver exceptions map to `DbRpcResponse.error_class` using the canonical Vertebra database exception names (`IntegrityError`, `OperationalError`, `ProgrammingError`).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_shm_broker.py`. Dependency category: Oracle 23ai via the `vtb_oracle_config` fixture when `VERTEBRA_CONFORMANCE_DOCKER=1`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Capsule registers | `pytest.importorskip("vertebra._oracle")`; `BrokerEngineRegistry.register(vertebra._oracle.broker_engine_capsule())` | Returns `4`; `4 in BrokerEngineRegistry.registered_tags()` |
| Normalization | `normalize(4, "SELECT :a FROM dual WHERE x = :a")` returns `("SELECT :a FROM dual WHERE x = :a", 1)` | As stated |
| Bad DSN | `BrokerEngineRegistry.open_pool(4, "not-a-dsn", 1)` | `RuntimeError` containing `"invalid oracle DSN"` |
| Unknown pool | `BrokerEngineRegistry.execute(4, 999, DbRpcCodec.encode_request(DbRpcRequest(version=1, op=DbRpcOp.EXECUTE, request_id=1, lease_slot=0, lease_generation=0, sql="SELECT 1")))` | `RuntimeError` containing `"unknown pool id 999"` |
| Execute round trip | `open_pool(4, "oracle://system:secret@127.0.0.1:1521/FREEPDB1", 1)` against Oracle 23ai via the `vtb_oracle_config` fixture when `VERTEBRA_CONFORMANCE_DOCKER=1`; execute `SELECT 1` frame | `DbRpcCodec.decode_response(...).rows == [[1]]` (skips with explicit reason when the live server is unavailable) |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k oracle`.
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --features tls,compression,gcp -- -D warnings`.
- `make lint` (stubtest for `vertebra._oracle`).

## Acceptance Criteria

- [ ] `vertebra._oracle.broker_engine_capsule()` registers tag `4` in `_vertebra` (R10).
- [ ] Every `DbRpcOp` executes on the native Oracle client (R10).

## Notes & Discoveries

- [2026-11-12 01:40] Oracle pools already expose session acquisition in `SyncOraclePool`; the backend reuses it rather than adding a second pool.

[^engine]: `src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool` — Oracle native surface.
[^capsule]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineCapsule` — Capsule contract.
