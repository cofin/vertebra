---
type: Task
id: native-redis-driver:ch03-python-cutover-and-notifications:t03
flow_id: native-redis-driver
chapter_id: ch03-python-cutover-and-notifications
title: Extract Self-Contained ValkeyQueueBackend into src/py/vertebra/framework/queues/valkey.py
description: Move ValkeyQueueBackend out of framework/queues/redis.py into a self-contained framework/queues/valkey.py module with a new ValkeyStreamClient protocol and zero imports from queues/redis.py.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-redis-driver:ch03-python-cutover-and-notifications:t02
files:
  - src/py/vertebra/framework/queues/valkey.py
  - src/py/vertebra/framework/queues/redis.py
  - src/py/vertebra/framework/queues/__init__.py
  - src/py/vertebra/framework/queues/backends.py
  - src/py/vertebra/framework/queues/base.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
read_set:
  - src/py/vertebra/framework/queues/redis.py
  - src/py/vertebra/framework/queues/__init__.py
  - src/py/tests/unit/framework/test_distributed_queues.py
estimated_loc: 220
tags:
  - queues
  - valkey
  - redis
  - isolation
sources:
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::ValkeyQueueBackend
    title: ValkeyQueueBackend currently defined in framework/queues/redis.py
  - id: queue-init
    resource: src/py/vertebra/framework/queues/__init__.py
    title: Framework queues package exports
verification_strategy: regression_tdd
---

# Task `t03`: Extract Self-Contained ValkeyQueueBackend into src/py/vertebra/framework/queues/valkey.py

## Objective & Scope Boundary

Extract the thin `ValkeyQueueBackend(RedisQueueBackend)` subclass from `src/py/vertebra/framework/queues/redis.py` into a self-contained `src/py/vertebra/framework/queues/valkey.py` module, define a new `ValkeyStreamClient` protocol there (copy of `RedisStreamClient`; none exists today), repoint the `src/py/vertebra/framework/queues/backends.py` import and the `QueueService._build_backend` dotted path in `src/py/vertebra/framework/queues/base.py`, and re-export `ValkeyQueueBackend` from `src/py/vertebra/framework/queues/__init__.py` per [`../spec.md` §2](../spec.md) [^queue-redis] [^queue-init].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/redis.py` — `RedisQueueBackend`, `ValkeyQueueBackend`, and `RedisStreamClient`.
- `src/py/vertebra/framework/queues/__init__.py` — Lazy queue backend exports (`PY-06`).
- `src/py/tests/unit/framework/test_distributed_queues.py` — Queue backend unit tests.

## Contracts & Edge Cases

- `src/py/vertebra/framework/queues/valkey.py` defines `ValkeyStreamClient` and `ValkeyQueueBackend`, keeping the current `redis.asyncio` client construction (`redis://`/`rediss://` or injected `client=`; scheme cutover to `valkey://`/`valkeys://`/`unix://` lands in `native-valkey-driver:ch03:t04`), with zero imports from `src/py/vertebra/framework/queues/redis.py`.
- `src/py/vertebra/framework/queues/redis.py` defines `RedisStreamClient` and `RedisQueueBackend` accepting `redis://`, `rediss://`, and `unix://` with zero references to Valkey.
- **Edge Case**: Legacy RESP3 bindings (`encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, `parse_cluster_redirect`) on `vertebra._vertebra` remain intact in this flow and are removed in `native-valkey-driver:ch03:t03` once `ValkeyCore` cuts over to `vertebra._valkey`.

## Test Matrix

- Assert in `test_distributed_queues.py` that `ValkeyQueueBackend` is imported from `vertebra.framework.queues.valkey` (and re-exported by `vertebra.framework.queues`), `queues/valkey.py` has zero imports from `queues/redis.py`, and the `backends.py` re-export and `QueueService` `kind="valkey"` path resolve to `vertebra.framework.queues.valkey.ValkeyQueueBackend`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py -q && uv run --no-sync ruff check src/py/vertebra/framework/queues && uv run --no-sync mypy src/py/vertebra/framework/queues
```

## Acceptance Criteria

- [ ] `src/py/vertebra/framework/queues/valkey.py` and `src/py/vertebra/framework/queues/redis.py` have zero cross-imports.
- [ ] `ValkeyQueueBackend` and `RedisQueueBackend` pass unit and type checks.
- [ ] Zero regressions in the fast verification gate.

[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::ValkeyQueueBackend` — ValkeyQueueBackend currently defined in framework/queues/redis.py.
[^queue-init]: `src/py/vertebra/framework/queues/__init__.py` — Framework queues package exports.
