---
type: Task
id: native-valkey-driver:ch03-python-cutover-and-notifications:t02
flow_id: native-valkey-driver
chapter_id: ch03-python-cutover-and-notifications
title: Implement Sync/Async Valkey Pub/Sub Notification Sources
description: Implement SyncValkeyNotificationSource and AsyncValkeyNotificationSource over vertebra._valkey Pub/Sub.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch03-python-cutover-and-notifications:t01
files:
  - src/py/vertebra/db/adapters/valkey/notify.py
  - src/py/vertebra/db/adapters/valkey/__init__.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
read_set:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/valkey/driver.py
  - src/py/vertebra/db/adapters/valkey/__init__.py
  - src/py/vertebra/_valkey.pyi
estimated_loc: 180
tags:
  - valkey
  - pubsub
  - notifications
sources:
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::SyncNotificationSource
    title: SyncNotificationSource and AsyncNotificationSource capability protocols
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
    title: SyncValkeyDriver and AsyncValkeyDriver
verification_strategy: regression_tdd
---

# Task `t02`: Implement Sync/Async Valkey Pub/Sub Notification Sources

## Objective & Scope Boundary

Implement `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` (`src/py/vertebra/db/adapters/valkey/notify.py`) over `vertebra._valkey.SyncValkeyListener` and `AsyncValkeyListener`, lazy-exporting them from `src/py/vertebra/db/adapters/valkey/__init__.py` per [`../spec.md` §2](../spec.md) [^driver-caps] [^valkey-drv].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncNotificationSource`, `AsyncNotificationSource`, and `NotificationEvent`.
- `src/py/vertebra/db/adapters/valkey/driver.py` — `SyncValkeyDriver` and `AsyncValkeyDriver`.
- `src/py/vertebra/db/adapters/valkey/__init__.py` — Lazy package exports (`PY-06`).
- `src/py/vertebra/_valkey.pyi` — `SyncValkeyListener` and `AsyncValkeyListener`.

## Contracts & Edge Cases

- Support `publish`, `subscribe`, `psubscribe`, `ssubscribe` (Cluster sharded pub/sub), `unsubscribe`, `listen()`, `close()`/`aclose()`, and optional `ChangeFeed` envelope hydration while recording pattern/shard metadata on `NotificationEvent.metadata`.
- Default to `ValkeyConfig()` and probe `ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")`.
- **Edge Case**: Breaking out of `listen()` closes the dedicated `SyncValkeyListener` / `AsyncValkeyListener` handle deterministically.

## Test Matrix

- Loopback tests in `test_redis_valkey_resp3.py` verify `isinstance(..., SyncNotificationSource | AsyncNotificationSource)`, channel/pattern/shard pub/sub, `ChangeFeed` hydration, and missing-wheel error hints.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_resp3.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/valkey && uv run --no-sync mypy src/py/vertebra/db/adapters/valkey
```

## Acceptance Criteria

- [ ] `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` pass unit, protocol, and type checks.
- [ ] Zero regressions in the fast verification gate.

[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::SyncNotificationSource` — `SyncNotificationSource` and `AsyncNotificationSource` capability protocols.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
