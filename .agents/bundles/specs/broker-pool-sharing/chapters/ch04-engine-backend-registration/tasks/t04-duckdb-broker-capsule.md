---
type: Task
id: "broker-pool-sharing:ch04:t04"
chapter_id: ch04-engine-backend-registration
title: DuckDB broker capsule
description: Implement BrokerPoolBackend for DuckDBBrokerBackend over the native DuckDB client and export vertebra._duckdb.broker_engine_capsule().
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch03:t02"
read_set:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/py/vertebra/_duckdb.pyi
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/_duckdb.pyi
  - src/py/tests/unit/extensions/test_shm_broker.py
files:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/py/vertebra/_duckdb.pyi
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
    resource: src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql
    title: DuckDB native normalization or pool surface
  - id: capsule
    resource: src/rs/vertebra-db/src/lib.rs::BrokerEngineCapsule
    title: Capsule contract from ch03
tags:
  - engines
  - duckdb
---

# Task t04: DuckDB Broker Capsule

## Objective & Scope Boundary

1. In `src/rs/vertebra-duckdb/src/lib.rs`, implement `DuckDBBrokerBackend` with `BrokerEngineBackend` (tag `DIALECT_TAG_DUCKDB = 6`, name `"duckdb"`, normalization via `normalize_named_sql` with `param_count` counting `$n` placeholders in the normalized text) and `BrokerPoolBackend` [^engine] [^capsule]: `open_pool` parses the DSN with the crate's existing config parser and creates one `SyncDuckDBClient` database handle per pool with per-lease cursors (DuckDB is in-process; `max_size` bounds concurrent cursors); `execute` decodes the `dbrpc` header (`vertebra_core::dbrpc::decode_header`) and msgspec-compatible body, runs the operation, and encodes a `DbRpcResponse`; `close_pool` drains and closes connections.
2. Add `#[pyfunction] broker_engine_capsule(py) -> PyResult<Bound<PyCapsule>>` calling `vertebra_db::new_broker_engine_pycapsule(py, Arc::new(DuckDBBrokerBackend::default()))` and register it in `_duckdb`.
3. Add `def broker_engine_capsule() -> object: ...` to `src/py/vertebra/_duckdb.pyi`.
4. Add the canonical tests below to `test_shm_broker.py`.

Out of scope: broker host dispatch (`ch05 / t03`), quarantine (`ch05 / t01`).

## Bounded Read Manifest

- `src/rs/vertebra-duckdb/src/lib.rs` — pymodule `_duckdb` and native client types.
- `src/py/vertebra/_duckdb.pyi` — normalization or pool surface [^engine].
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend`, `new_broker_engine_pycapsule` [^capsule].
- `src/py/vertebra/_duckdb.pyi`, `src/py/tests/unit/extensions/test_shm_broker.py`.

## Symbol & Signature Contracts

- `#[derive(Default)] pub struct DuckDBBrokerBackend { pools: DashMap<u64, ...>, next_id: AtomicU64 }`.
- `fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String>`; ids start at `1`.
- `fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String>` supports every `DbRpcOp`; `Savepoint` uses the engine's savepoint SQL; `Cancel` uses the engine's native cancel.
- Python: `vertebra._duckdb.broker_engine_capsule() -> object` (a `PyCapsule` named `vertebra.BrokerEngineCapsule`).

## Edge Cases & Error Semantics

- Invalid DSN: `Err("invalid duckdb DSN: <reason>")`.
- Unknown pool id: `Err("unknown pool id <id>")`.
- `max_size = 0`: `Err("max_size must be >= 1")`.
- Driver exceptions map to `DbRpcResponse.error_class` using the canonical Vertebra database exception names (`IntegrityError`, `OperationalError`, `ProgrammingError`).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_shm_broker.py`. Dependency category: in-process `duckdb:///:memory:` (no container).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Capsule registers | `pytest.importorskip("vertebra._duckdb")`; `BrokerEngineRegistry.register(vertebra._duckdb.broker_engine_capsule())` | Returns `6`; `6 in BrokerEngineRegistry.registered_tags()` |
| Normalization | `normalize(6, "SELECT :a")` returns the `normalize_named_sql` output with count `1` | As stated |
| Bad DSN | `BrokerEngineRegistry.open_pool(6, "not-a-dsn", 1)` | `RuntimeError` containing `"invalid duckdb DSN"` |
| Unknown pool | `BrokerEngineRegistry.execute(6, 999, DbRpcCodec.encode_request(DbRpcRequest(version=1, op=DbRpcOp.EXECUTE, request_id=1, lease_slot=0, lease_generation=0, sql="SELECT 1")))` | `RuntimeError` containing `"unknown pool id 999"` |
| Execute round trip | `open_pool(6, "duckdb:///:memory:", 1)` against in-process `duckdb:///:memory:` (no container); execute `SELECT 1` frame | `DbRpcCodec.decode_response(...).rows == [[1]]` (skips with explicit reason when the live server is unavailable) |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k duckdb`.
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-duckdb --features tls,compression,gcp -- -D warnings`.
- `make lint` (stubtest for `vertebra._duckdb`).

## Acceptance Criteria

- [ ] `vertebra._duckdb.broker_engine_capsule()` registers tag `6` in `_vertebra` (R10).
- [ ] Every `DbRpcOp` executes on the native DuckDB client (R10).

## Notes & Discoveries

- [2026-11-12 01:40] A `:memory:` DSN is per pool; workers sharing the pool see the same in-memory database, which is the point of broker hosting for DuckDB.

[^engine]: `src/rs/vertebra-duckdb/src/lib.rs::normalize_named_sql` — DuckDB native surface.
[^capsule]: `src/rs/vertebra-db/src/lib.rs::BrokerEngineCapsule` — Capsule contract.
