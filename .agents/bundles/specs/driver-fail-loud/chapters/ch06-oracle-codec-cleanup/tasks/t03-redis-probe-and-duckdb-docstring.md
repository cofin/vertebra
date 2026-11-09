---
type: Task
id: "driver-fail-loud:ch06:t03"
chapter_id: ch06-oracle-codec-cleanup
title: Remove the redis_hash_slot_native probe and rewrite the DuckDB package docstring
description: Delete the redis_hash_slot_native fallback probe from RedisCore.hash_slot, and rewrite the DuckDB package docstring to describe the duckdb-package driver without the future import.
status: stable
state: pending
priority: P2
verification_strategy: static_validation
depends_on: []
read_set:
  - src/py/vertebra/db/adapters/redis/core.py
  - src/py/vertebra/db/adapters/duckdb/__init__.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
files:
  - src/py/vertebra/db/adapters/redis/core.py
  - src/py/vertebra/db/adapters/duckdb/__init__.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
estimated_loc: 5
plan_revision: 2
plan_commit: fd571e3a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: redis-core
    resource: src/py/vertebra/db/adapters/redis/core.py::RedisCore.hash_slot
    title: Redis hash slot with redis_hash_slot_native probe
  - id: duckdb-init
    resource: src/py/vertebra/db/adapters/duckdb/__init__.py
    title: DuckDB adapter package docstring
  - id: resp3-test
    resource: src/py/tests/unit/db/test_redis_valkey_resp3.py
    title: RESP3 codec and hash slot tests
tags:
  - python
  - redis
  - duckdb
  - cleanup
---

# Task t03: Remove the `redis_hash_slot_native` Probe and Rewrite the DuckDB Package Docstring

## Objective & Scope Boundary

Delete the `or getattr(native, "redis_hash_slot_native", None)` clause from `RedisCore.hash_slot` (`_vertebra` exports `redis_hash_slot` and never exported `redis_hash_slot_native`), and rewrite the `duckdb/__init__.py` module docstring, which describes SHM single-writer/multi-reader coordination, to describe the `duckdb`-package driver; drop `from __future__ import annotations` from `duckdb/__init__.py`.[^redis-core][^duckdb-init]

Precondition: ch02 t04 (DuckDB requires the `duckdb` package) and ch04 t01 (`test_redis_valkey_resp3.py` retargeted onto `resp3_server`) are completed; the docstring below describes ch02 t04's driver, and the Final command runs ch04 t01's test file.

Out of scope: every other `redis/core.py` line, including its future import (owned by `native-redis-driver`); `RedisCore.parse_cluster_redirect` (probes the exported `parse_cluster_redirect`; stays); DuckDB driver code and field-level docstrings.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/redis/core.py` — `RedisCore.hash_slot`.[^redis-core]
- `src/py/vertebra/db/adapters/duckdb/__init__.py` — module docstring, future import, `TYPE_CHECKING` block.[^duckdb-init]
- `src/py/tests/unit/db/test_redis_valkey_resp3.py` — hash-slot tests.[^resp3-test]
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — DuckDB import block.

Grep-only verification: `grep -rn "redis_hash_slot_native" src/py` lists only the line this task edits.

## Symbol & Signature Contracts

- `redis/core.py::RedisCore.hash_slot(key: str | bytes) -> int`: `native_fn = getattr(native, "redis_hash_slot", None)`; the pure-Python CRC16 fallback is unchanged.
- `duckdb/__init__.py`:
  - Module docstring: `"""DuckDB driver over the `duckdb` package with bootstrap statements and shared-memory Arrow IPC helpers."""`.
  - Delete `from __future__ import annotations`. Names imported under `TYPE_CHECKING` appear only in that import block and in the `__all__` / `_EXPORT_MAP` string literals, never in an annotation, so no quoting is needed.

## Edge Cases & Error Semantics

- `_vertebra` never exported `redis_hash_slot_native`, so the deleted clause never supplied a function; `hash_slot` output is unchanged with and without the built extension.
- The only annotation in `duckdb/__init__.py` is `_EXPORT_MAP: dict[str, tuple[str, str]]`, which names builtins only; removing the future import changes no runtime name resolution.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_redis_valkey_resp3.py`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py` (in-process; no test edits).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Hash slot with native | `RedisCore.hash_slot("123456789")` | `12739` |
| Hash tag | `RedisCore.hash_slot("user:{42}:profile") == RedisCore.hash_slot(b"42")` | `True` |
| Import without future | `uv run --no-sync python -c "import vertebra.db.adapters.duckdb"` | exits 0 |
| Probe residue | `grep -rn "redis_hash_slot_native" src/py` | no output, exit 1 |

## Verification

- Baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_resp3.py -q` passes.
- Final: `uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/db/test_enterprise_olap_drivers.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/redis/core.py src/py/vertebra/db/adapters/duckdb/__init__.py && uv run --no-sync mypy src/py/vertebra/db/adapters/redis/core.py src/py/vertebra/db/adapters/duckdb/__init__.py && make rs-test`
- Residue: `grep -rn "redis_hash_slot_native" src/py` prints nothing; `grep -n "from __future__" src/py/vertebra/db/adapters/duckdb/__init__.py` prints nothing.

## Acceptance Criteria

- [ ] `RedisCore.hash_slot` probes only `redis_hash_slot` (R9.2).
- [ ] The DuckDB package docstring describes the `duckdb`-package driver and carries no SHM-brokered claim; no future import remains in `duckdb/__init__.py` (R9.2).
- [ ] Redis hash-slot and DuckDB tests pass unchanged; `make rs-test` exits 0.

## Notes & Discoveries

- [2026-11-07 00:00] `RedisCore.parse_cluster_redirect` probes `parse_cluster_redirect`, which `_vertebra` exports; that probe stays.

[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::RedisCore.hash_slot` — Redis hash slot with redis_hash_slot_native probe.
[^duckdb-init]: `src/py/vertebra/db/adapters/duckdb/__init__.py` — DuckDB adapter package docstring.
[^resp3-test]: `src/py/tests/unit/db/test_redis_valkey_resp3.py` — RESP3 codec and hash slot tests.
