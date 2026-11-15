---
type: Task
id: native-valkey-driver:ch04-conformance-and-docs:t01
flow_id: native-valkey-driver
chapter_id: ch04-conformance-and-docs
title: Add Valkey 9.0 Hash-Field Expiration and Redirect Loopback Tests
description: Add unit and loopback tests for Valkey 9.0 HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT/HTTL/HPTTL/HPERSIST, CLIENT CAPA redirect, and CLUSTER SHARDS replica routing.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch03-python-cutover-and-notifications:t04
files:
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
read_set:
  - src/py/tests/unit/db/test_redis_valkey_driver.py
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
  - src/py/vertebra/db/adapters/valkey/driver.py
estimated_loc: 180
tags:
  - valkey
  - testing
  - ttl
  - cluster
sources:
  - id: test-drv
    resource: src/py/tests/unit/db/test_redis_valkey_driver.py
    title: Unit tests for Redis and Valkey drivers
  - id: test-resp3
    resource: src/py/tests/unit/db/test_redis_valkey_resp3.py
    title: RESP3, tracking, and pub/sub unit tests
verification_strategy: regression_tdd
---

# Task `t01`: Add Valkey 9.0 Hash-Field Expiration and Redirect Loopback Tests

## Objective & Scope Boundary

Add parameterized unit and loopback tests in `src/py/tests/unit/db/test_redis_valkey_driver.py` and `src/py/tests/unit/db/test_redis_valkey_resp3.py` covering Valkey 9.0 hash-field expiration (`hexpire`, `hpexpire`, `hexpireat`, `hpexpireat`, `httl`, `hpttl`, `hpersist`), `CLIENT CAPA redirect`, and `CLUSTER SHARDS` replica routing per [`../spec.md` §2](../spec.md) [^test-drv] [^test-resp3].

## Bounded Read Manifest

- `src/py/tests/unit/db/test_redis_valkey_driver.py` — Driver unit tests.
- `src/py/tests/unit/db/test_redis_valkey_resp3.py` — RESP3, tracking, and pub/sub tests.
- `src/py/vertebra/db/adapters/valkey/driver.py` — `SyncValkeyDriver` and `AsyncValkeyDriver`.

## Contracts & Edge Cases

- Use `@pytest.mark.parametrize` for `NX`/`XX`/`GT`/`LT` hash-field expiration conditions and invalid-condition `ProgrammingError` checks.
- Verify that Valkey 8.x `-ERR unknown command 'HEXPIRE'` maps cleanly to `ProgrammingError`.

## Test Matrix

- Run `pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py -q
```

## Acceptance Criteria

- [ ] Valkey 9.0 hash-field expiration, `CLIENT CAPA redirect`, and `CLUSTER SHARDS` tests pass.
- [ ] Zero regressions in the fast verification gate.

[^test-drv]: `src/py/tests/unit/db/test_redis_valkey_driver.py` — Unit tests for Redis and Valkey drivers.
[^test-resp3]: `src/py/tests/unit/db/test_redis_valkey_resp3.py` — RESP3, tracking, and pub/sub unit tests.
