---
type: Task
id: "broker-pool-sharing:ch04:t01"
chapter_id: ch04-engine-backend-registration
title: MySQL broker capsule
description: Implement BrokerPoolBackend for MySQLBrokerBackend over the native MySQL client and export vertebra._mysql.broker_engine_capsule().
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch03:t01"
read_set:
  - src/rs/vertebra-mysql/src/lib.rs
  - src/rs/vertebra-mysql/src/protocol.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/_mysql.pyi
  - src/py/tests/unit/extensions/test_shm_broker.py
files:
  - src/rs/vertebra-mysql/src/lib.rs
  - src/py/vertebra/_mysql.pyi
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
    resource: src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend
    title: MySQL native normalization or pool surface
  - id: capsule
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineCapsule
    title: Capsule contract from ch03
tags:
  - engines
  - mysql
---

# Task t01: MySQL Broker Capsule

## Objective & Scope Boundary

1. In `src/rs/vertebra-mysql/src/lib.rs`, implement `MySQLBrokerBackend` with `BrokerEngineBackend` (tag `DIALECT_TAG_MYSQL = 2`, name `"mysql"`, normalization via `protocol.rs::normalize_mysql_placeholders` (existing `MySQLBrokerBackend::normalize_statement`)) and `BrokerPoolBackend` [^engine] [^capsule]: `open_pool` parses the DSN with the crate's existing config parser and creates `SyncMySQLClient` connections held in a bounded per-pool `Vec` guarded by a `parking_lot::Mutex`; `execute` decodes the `dbrpc` header (`vertebra_core::dbrpc::decode_header`) and msgspec-compatible body, runs the operation, and encodes a `DbRpcResponse`; `close_pool` drains and closes connections.
2. Add `#[pyfunction] broker_engine_capsule(py) -> PyResult<Bound<PyCapsule>>` calling `vertebra_db::new_broker_engine_pycapsule(py, Arc::new(MySQLBrokerBackend::default()))` and register it in `_mysql`.
3. Add `def broker_engine_capsule() -> object: ...` to `src/py/vertebra/_mysql.pyi`.
4. Add the canonical tests below to `test_shm_broker.py`.

Out of scope: broker host dispatch (`ch05 / t03`), quarantine (`ch05 / t01`).

## Bounded Read Manifest

- `src/rs/vertebra-mysql/src/lib.rs` — pymodule `_mysql` and native client types.
- `src/rs/vertebra-mysql/src/protocol.rs` — normalization or pool surface [^engine].
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend`, `new_broker_engine_pycapsule` [^capsule].
- `src/py/vertebra/_mysql.pyi`, `src/py/tests/unit/extensions/test_shm_broker.py`.

## Symbol & Signature Contracts

- `#[derive(Default)] pub struct MySQLBrokerBackend { pools: DashMap<u64, ...>, next_id: AtomicU64 }`.
- `fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String>`; ids start at `1`.
- `fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String>` supports every `DbRpcOp`; `Savepoint` uses the engine's savepoint SQL; `Cancel` uses the engine's native cancel.
- Python: `vertebra._mysql.broker_engine_capsule() -> object` (a `PyCapsule` named `vertebra.BrokerEngineCapsule`).

## Edge Cases & Error Semantics

- Invalid DSN: `Err("invalid mysql DSN: <reason>")`.
- Unknown pool id: `Err("unknown pool id <id>")`.
- `max_size = 0`: `Err("max_size must be >= 1")`.
- Driver exceptions map to `DbRpcResponse.error_class` using the canonical Vertebra database exception names (`IntegrityError`, `OperationalError`, `ProgrammingError`).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_shm_broker.py`. Dependency category: MySQL loopback `vertebra._mysql.MySQLWireTestServer`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Capsule registers | `pytest.importorskip("vertebra._mysql")`; `BrokerEngineRegistry.register(vertebra._mysql.broker_engine_capsule())` | Returns `2`; `2 in BrokerEngineRegistry.registered_tags()` |
| Normalization | `normalize(2, "SELECT :a, :b")` returns `("SELECT ?, ?", 2)` | As stated |
| Bad DSN | `BrokerEngineRegistry.open_pool(2, "not-a-dsn", 1)` | `RuntimeError` containing `"invalid mysql DSN"` |
| Unknown pool | `BrokerEngineRegistry.execute(2, 999, DbRpcCodec.encode_request(DbRpcRequest(version=1, op=DbRpcOp.EXECUTE, request_id=1, lease_slot=0, lease_generation=0, sql="SELECT 1")))` | `RuntimeError` containing `"unknown pool id 999"` |
| Execute round trip | `open_pool(2, "mysql://vtb:secret@127.0.0.1:3306/vtb", 1)` against MySQL loopback `vertebra._mysql.MySQLWireTestServer`; execute `SELECT 1` frame | `DbRpcCodec.decode_response(...).rows == [[1]]` (skips with explicit reason when the live server is unavailable) |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k mysql`.
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mysql --features tls,compression,gcp -- -D warnings`.
- `make lint` (stubtest for `vertebra._mysql`).

## Acceptance Criteria

- [ ] `vertebra._mysql.broker_engine_capsule()` registers tag `2` in `_vertebra` (R10).
- [ ] Every `DbRpcOp` executes on the native MySQL client (R10).

## Notes & Discoveries

- [2026-11-12 01:40] `MySQLBrokerBackend` already implements `BrokerEngineBackend`; this task adds `BrokerPoolBackend` and removes nothing else.

[^engine]: `src/rs/vertebra-mysql/src/lib.rs::MySQLBrokerBackend` — MySQL native surface.
[^capsule]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineCapsule` — Capsule contract.
