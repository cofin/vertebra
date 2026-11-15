---
type: Task
id: native-valkey-driver:ch03-python-cutover-and-notifications:t04
flow_id: native-valkey-driver
chapter_id: ch03-python-cutover-and-notifications
title: Cut ValkeyQueueBackend Over to vertebra._valkey Streams and ZSETs
description: Wire ValkeyQueueBackend in src/py/vertebra/framework/queues/valkey.py onto native vertebra._valkey stream consumer groups and delayed ZSETs.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch03-python-cutover-and-notifications:t03
files:
  - src/py/vertebra/framework/queues/valkey.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
read_set:
  - src/py/vertebra/framework/queues/redis.py
  - src/py/vertebra/_valkey.pyi
  - src/py/tests/unit/framework/test_distributed_queues.py
estimated_loc: 180
tags:
  - queues
  - streams
  - valkey
sources:
  - id: queue-valkey
    resource: src/py/vertebra/framework/queues/redis.py::ValkeyQueueBackend
    title: ValkeyQueueBackend for Valkey Streams and delayed ZSETs
verification_strategy: regression_tdd
---

# Task `t04`: Cut ValkeyQueueBackend Over to vertebra._valkey Streams and ZSETs

## Objective & Scope Boundary

Replace `redis.asyncio` in `src/py/vertebra/framework/queues/valkey.py` with `_ValkeyStreamAdapter` wrapping `vertebra._valkey.AsyncValkeyClient` per [`../spec.md` §2](../spec.md) [^queue-valkey].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/redis.py` — Reference queue backend structure.
- `src/py/vertebra/_valkey.pyi` — `AsyncValkeyClient` stream and Lua script methods.
- `src/py/tests/unit/framework/test_distributed_queues.py` — Distributed queue unit tests.

## Contracts & Edge Cases

- Probe `ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")` in `ValkeyQueueBackend`.
- Accept `valkey://`, `valkeys://`, and `unix://` while rejecting `memory://`, `redis://`, and `rediss://` and preserving `client: ValkeyStreamClient | None = None`.
- **Edge Case**: Never use a `Native*` class prefix (`_ValkeyStreamAdapter` complies with `PY-07`).

## Test Matrix

- `@pytest.mark.parametrize` covers `valkey://`, `valkeys://`, and `unix://`, `memory://` and `redis://` rejection, missing-wheel `MissingDependencyError` hints, and loopback enqueue/consume/ack/autoclaim/DLQ.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py -q && uv run --no-sync ruff check src/py/vertebra/framework/queues/valkey.py && uv run --no-sync mypy src/py/vertebra/framework/queues/valkey.py
```

## Acceptance Criteria

- [ ] Zero `redis.asyncio` references remain anywhere in `src/py/vertebra/`.
- [ ] Zero regressions in the fast verification gate.

[^queue-valkey]: `src/py/vertebra/framework/queues/redis.py::ValkeyQueueBackend` — ValkeyQueueBackend for Valkey Streams and delayed ZSETs.
