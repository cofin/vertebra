---
type: Task
id: "queues-tasks-and-channels:ch04-litestar-queues-integration:t01"
flow_id: queues-tasks-and-channels
chapter_id: ch04-litestar-queues-integration
title: "Bump litestar-queues>=0.14.0 and Implement Rust-Ring SHMQueueBackend(BaseQueueBackend)"
description: "Raise litestar-queues>=0.14.0 in pyproject.toml and build slotted SHMQueueBackend over vertebra._vertebra SHM rings."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch02-first-party-queue-backends-and-service:t01"
files:
  - pyproject.toml
  - src/py/vertebra/extensions/litestar/queues/shm_backend.py
tests:
  - src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
read_set:
  - pyproject.toml
  - src/py/vertebra/extensions/litestar/queues/backend.py
  - src/py/vertebra/extensions/litestar/queues/config.py
estimated_loc: 240
tags:
  - litestar-queues
  - shm
  - queues
sources:
  - id: pyproject-lq
    resource: pyproject.toml
    title: "litestar-queues version floor in litestar extra"
  - id: lq-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
    title: "Existing litestar-queues backend module with broken litestar_queues.records import"
---

# Task `t01`: Bump litestar-queues>=0.14.0 and Implement Rust-Ring SHMQueueBackend(BaseQueueBackend)

## Objective & Scope Boundary

Bump `litestar-queues>=0.14.0` in `pyproject.toml` and implement slotted `SHMQueueBackend(BaseQueueBackend)` + `SHMQueueBackendConfig` (`@queue_backend('vertebra_shm')`) in `shm_backend.py` per [`../spec.md`](../spec.md) [^pyproject-lq] [^lq-backend].

## Bounded Read Manifest

- `pyproject.toml` — `[project.optional-dependencies].litestar`.
- `src/py/vertebra/extensions/litestar/queues/backend.py` — existing `SHMQueueBackend` and broken `litestar_queues.records` import.

## Contracts & Edge Cases

- Raise `litestar-queues>=0.14.0` in `pyproject.toml`.
- Implement `__slots__`-equipped `SHMQueueBackend(BaseQueueBackend)` + `SHMQueueBackendConfig` over `vertebra._vertebra` SHM rings and futex/ulock wakeups (zero `asyncio.Queue` fake), supporting `list_tasks` pagination, `(status='running', retry_count)` fencing, `terminal_ttl` cleanup, uniqueness reservations, and maintenance leases.

## Test Matrix

- Parametrized `litestar-queues==0.14.0` `BaseQueueBackend` contract tests covering `enqueue`, fenced `claim_many`/`complete_task`/`fail_task`, `list_tasks` pagination, `terminal_ttl`, and futex wakeups.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/litestar/queues pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/litestar/queues && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
```

## Acceptance Criteria

- [ ] `pyproject.toml` requires `litestar-queues>=0.14.0` and `SHMQueueBackend` passes strict `mypy` (zero `ignore_missing_imports` masking) and `BaseQueueBackend` contract checks.
- [ ] Zero regressions in the chapter fast verification gate.

[^pyproject-lq]: `pyproject.toml` — litestar-queues version floor in litestar extra.
[^lq-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — Existing litestar-queues backend module with broken litestar_queues.records import.
