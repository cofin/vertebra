---
type: Task
id: "broker-pool-sharing:ch02:t01"
chapter_id: ch02-pool-partitions-and-dbrpc
title: Stable cross-process statement and bind hash
description: Add XXH3-64 stable_hash64 in vertebra-core, expose it from vertebra._vertebra, and replace every process-seeded Python hash() used for statement and bind keys in the broker transport.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/extensions/test_broker_transport.py
files:
  - src/rs/vertebra-core/src/broker.rs
  - src/rs/vertebra-py/src/broker.rs
  - src/py/vertebra/extensions/broker/transport.py
tests:
  - src/py/tests/unit/extensions/test_broker_transport.py
estimated_loc: 90
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
  - id: shm-plan
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport._get_or_compile_plan
    title: sql_hash=hash((dialect_tag, raw_sql))
  - id: acquire-sync
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport._acquire_sync
    title: bind_hash = hash(resolved_bind_key)
  - id: acquire-async
    resource: src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport._acquire_async
    title: bind_hash = hash(bind_key)
tags:
  - hashing
---

# Task t01: Stable Cross-Process Statement and Bind Hash

## Objective & Scope Boundary

1. Add `xxhash-rust = { version = "0.8", features = ["xxh3"] }` to `vertebra-core` (workspace dependency edit in `src/rs/vertebra-core/Cargo.toml` is a one-line manifest change accompanying `broker.rs`) and `pub fn stable_hash64(bytes: &[u8]) -> u64 { xxhash_rust::xxh3::xxh3_64(bytes) }` in `src/rs/vertebra-core/src/broker.rs`. Reason for the dependency: XXH3 is the documented stable key function; no existing dependency provides it.
2. Expose `#[pyfunction] stable_hash64(data: &Bound<PyAny>) -> PyResult<u64>` from `src/rs/vertebra-py/src/broker.rs` (accepts `bytes` or `str` via the existing `extract_bytes_or_str`) and register it on `_vertebra`; add the stub `def stable_hash64(data: bytes | str) -> int: ...` to `src/py/vertebra/_vertebra.pyi`.
3. In `src/py/vertebra/extensions/broker/transport.py` [^shm-plan] [^acquire-sync] [^acquire-async], replace `hash(key)`, `hash((dialect_tag, raw_sql))`, `hash(resolved_bind_key) & 0xFFFF_FFFF_FFFF_FFFF`, and `hash(bind_key) & ...` with `stable_hash64(...)` over the canonical byte encodings below. The in-memory site is deleted in `t04`; until then it uses the same function.

## Bounded Read Manifest

- `src/py/vertebra/extensions/broker/transport.py` — the four hash sites.
- `src/rs/vertebra-py/src/broker.rs` — `extract_bytes_or_str`, module registration.
- `src/rs/vertebra-core/src/broker.rs` — module surface.
- `src/py/vertebra/_vertebra.pyi`.
- `src/py/tests/unit/extensions/test_broker_transport.py`.

## Symbol & Signature Contracts

- Statement key bytes: `dialect_tag.to_bytes(2, "little") + raw_sql.encode("utf-8")`.
- Bind key bytes: `bind_key.encode("utf-8")`.
- `StatementPlan.sql_hash` keeps type `int` (unsigned 64-bit).

## Edge Cases & Error Semantics

- Non-`str`/`bytes` input: `TypeError` from `extract_bytes_or_str`.
- Empty string: valid, returns `xxh3_64(b"") == 0x2D06800538D394C2`.
- Non-ASCII SQL: UTF-8 encoded; identical across processes.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_broker_transport.py`. Dependency category: in-process plus subprocess.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Known vector | `stable_hash64(b"")` | `0x2D06800538D394C2` |
| str equals bytes | `stable_hash64("SELECT 1")` vs `stable_hash64(b"SELECT 1")` | Equal |
| Cross-seed | `subprocess.run([sys.executable, "-c", "from vertebra._vertebra import stable_hash64; print(stable_hash64('k'))"], env={**os.environ, "PYTHONHASHSEED": seed})` parametrized `seed` in `("1", "2")` | Both outputs equal |
| Plan key | `SyncSHMBrokerTransport` plan for dialect `postgres`, `"SELECT 1"` | `plan.sql_hash == stable_hash64((1).to_bytes(2, "little") + b"SELECT 1")` |
| No hash() | `git grep -nE "\bhash\(" -- src/py/vertebra/extensions/broker/transport.py` | No matches |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_broker_transport.py -q`.
- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --features tls,compression,gcp broker`.
- `make lint` (stubtest included).

## Acceptance Criteria

- [ ] Every cross-process statement and bind key uses `stable_hash64` (R4).
- [ ] The cross-seed test passes (R4).

## Notes & Discoveries

- [2026-11-12 01:40] `xxhash-rust` is not in `src/rs/Cargo.lock` at HEAD; `fnv` exists only in `vertebra-http`.

[^shm-plan]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport._get_or_compile_plan` — Statement key.
[^acquire-sync]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport._acquire_sync` — Sync bind key.
[^acquire-async]: `src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport._acquire_async` — Async bind key.
