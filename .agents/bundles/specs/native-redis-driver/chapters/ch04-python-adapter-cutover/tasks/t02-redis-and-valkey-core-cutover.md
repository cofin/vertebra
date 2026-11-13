---
type: Task
id: "native-redis-driver:ch04:t02"
flow_id: native-redis-driver
chapter_id: ch04-python-adapter-cutover
title: "Delete Legacy Resp3Parser and Delegate Slot/Redirect Helpers to vertebra._redis"
description: "Remove Resp3Parser/Resp3Error/encode_* from redis/core.py, valkey/core.py, and redis/__init__.py; delegate to _redis."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch04:t01"
files:
  - src/py/vertebra/db/adapters/redis/core.py
  - src/py/vertebra/db/adapters/valkey/core.py
  - src/py/vertebra/db/adapters/redis/__init__.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
read_set:
  - src/py/vertebra/db/adapters/redis/core.py
  - src/py/vertebra/db/adapters/valkey/core.py
  - src/py/vertebra/db/adapters/redis/__init__.py
  - src/py/vertebra/_redis.pyi
estimated_loc: 140
tags:
  - redis
  - valkey
  - core
  - debloat
sources:
  - id: redis-core
    resource: src/py/vertebra/db/adapters/redis/core.py::Resp3Parser
    title: "Pure-Python Resp3Parser and RESP command encoders"
  - id: valkey-core
    resource: src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore
    title: "ValkeyCore re-exporting Redis RESP3 helpers"
---

# Task `t02`: Delete Legacy Resp3Parser and Delegate Slot/Redirect Helpers to vertebra._redis

## Objective & Scope Boundary

Delete `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` across `redis/core.py`, `valkey/core.py`, and `redis/__init__.py`, delegating `crc16_xmodem`, `hash_slot`, and `parse_cluster_redirect` to `vertebra._redis` per [`../spec.md` §2](../spec.md) [^redis-core] [^valkey-core].

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/redis/core.py` — `Resp3Parser`, `Resp3Error`, `encode_*`, `crc16_xmodem`, and `parse_cluster_redirect`.
- `src/py/vertebra/db/adapters/valkey/core.py` — Re-exports from `redis.core`.
- `src/py/vertebra/db/adapters/redis/__init__.py` — Lazy exports (`PY-06`).
- `src/py/vertebra/_redis.pyi` — `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect`.

## Contracts & Edge Cases

- Probe `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")` with zero pure-Python fallback (`PY-07`).
- **Edge Case**: Update `valkey/core.py` in the same task so `from vertebra.db.adapters.valkey import ValkeyCore` never hits `ImportError`.

## Test Matrix

- `@pytest.mark.parametrize` verifies `crc16_xmodem`, `hash_slot`, and `parse_cluster_redirect` via `RedisCore` and `ValkeyCore`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_resp3.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/redis src/py/vertebra/db/adapters/valkey
```

## Acceptance Criteria

- [ ] Pure-Python `Resp3Parser` and command encoders are removed cleanly.
- [ ] Zero regressions in the chapter fast verification gate.

[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — Pure-Python Resp3Parser and RESP command encoders.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore re-exporting Redis RESP3 helpers.
