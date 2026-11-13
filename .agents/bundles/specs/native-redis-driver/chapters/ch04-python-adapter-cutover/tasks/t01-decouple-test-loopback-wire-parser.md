---
type: Task
id: "native-redis-driver:ch04:t01"
flow_id: native-redis-driver
chapter_id: ch04-python-adapter-cutover
title: "Decouple conftest.py::Resp3FakeServer from Production Resp3Parser"
description: "Replace Resp3Parser import in src/py/tests/unit/conftest.py with a self-contained test framing helper."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch03:t04"
files: []
tests:
  - src/py/tests/unit/conftest.py
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
read_set:
  - src/py/tests/unit/conftest.py
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
estimated_loc: 165
tags:
  - redis
  - tests
  - resp3
sources:
  - id: unit-conftest
    resource: src/py/tests/unit/conftest.py::Resp3FakeServer
    title: "Resp3FakeServer loopback test fixture coupled to Resp3Parser"
---

# Task `t01`: Decouple conftest.py::Resp3FakeServer from Production Resp3Parser

## Objective & Scope Boundary

Eliminate `from vertebra.db.adapters.redis.core import Resp3Parser` in `src/py/tests/unit/conftest.py` BEFORE deleting `Resp3Parser` in `t02`, and migrate unit tests from `socket_factory` / `Resp3Error` onto loopback TCP and `DatabaseError` per [`../spec.md` §2](../spec.md) [^unit-conftest].

## Bounded Read Manifest

- `src/py/tests/unit/conftest.py` — `Resp3FakeServer` top-level `from vertebra.db.adapters.redis.core import Resp3Parser`.
- `src/py/tests/unit/db/test_redis_valkey_driver.py` — `socket_factory` and loopback driver tests.
- `src/py/tests/unit/db/test_redis_valkey_resp3.py` — RESP3 protocol and redirect tests.

## Contracts & Edge Cases

- Implement a compact test-only `_parse_wire_frame` helper inside `src/py/tests/unit/conftest.py` so `Resp3FakeServer` has zero imports from `vertebra.db.adapters.redis.core`.
- **Edge Case**: Ensure pytest collection across all of `src/py/tests/unit/` succeeds even when `Resp3Parser` is absent.

## Test Matrix

- `@pytest.mark.parametrize` verifies `Resp3FakeServer` framing across bulk arrays, inline commands, and pipelined bursts.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py -q
```

## Acceptance Criteria

- [ ] `src/py/tests/unit/conftest.py` has zero `Resp3Parser` imports from production modules.
- [ ] Zero regressions in the chapter fast verification gate.

[^unit-conftest]: `src/py/tests/unit/conftest.py::Resp3FakeServer` — Resp3FakeServer loopback test fixture coupled to Resp3Parser.
