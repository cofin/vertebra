---
type: Task
id: native-valkey-driver:ch03-python-cutover-and-notifications:t01
flow_id: native-valkey-driver
chapter_id: ch03-python-cutover-and-notifications
title: Cut Python SyncValkeyDriver and AsyncValkeyDriver Over to vertebra._valkey
description: Delegate ValkeyConfig, ValkeyCore, SyncValkeyDriver, and AsyncValkeyDriver to vertebra._valkey and expose Valkey 9.0 hash-field expiration methods.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch02-rust-clients-topologies-and-streams:t04
files:
  - src/py/vertebra/db/adapters/valkey/config.py
  - src/py/vertebra/db/adapters/valkey/core.py
  - src/py/vertebra/db/adapters/valkey/driver.py
  - src/py/vertebra/db/adapters/valkey/__init__.py
tests:
  - src/py/tests/unit/db/test_redis_valkey_driver.py
read_set:
  - src/py/vertebra/db/adapters/valkey/config.py
  - src/py/vertebra/db/adapters/valkey/core.py
  - src/py/vertebra/db/adapters/valkey/driver.py
  - src/py/vertebra/db/adapters/valkey/__init__.py
  - src/py/vertebra/_valkey.pyi
estimated_loc: 230
tags:
  - python
  - valkey
  - pyo3
  - cutover
sources:
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
    title: SyncValkeyDriver and AsyncValkeyDriver
  - id: valkey-cfg
    resource: src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig
    title: ValkeyConfig
  - id: valkey-core
    resource: src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore
    title: ValkeyCore RESP3 command builder and hash slot helpers
verification_strategy: regression_tdd
---

# Task `t01`: Cut Python SyncValkeyDriver and AsyncValkeyDriver Over to vertebra._valkey

## Objective & Scope Boundary

Replace Python `socket`/`asyncio` I/O in `src/py/vertebra/db/adapters/valkey/{config,core,driver,__init__}.py` with thin delegates over `vertebra._valkey` (`SyncValkeyClient`, `AsyncValkeyClient`, `SyncValkeyPipeline`, `AsyncValkeyPipeline`, `valkey_hash_slot`, `crc16_xmodem`, `parse_cluster_redirect`) and expose Valkey 9.0 hash-field expiration methods (`hexpire`, `hpexpire`, `hexpireat`, `hpexpireat`, `httl`, `hpttl`, `hpersist`) per [`../spec.md` §2](../spec.md) [^valkey-drv] [^valkey-cfg] [^valkey-core].

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/valkey/config.py` — `ValkeyConfig`.
- `src/py/vertebra/db/adapters/valkey/core.py` — `ValkeyCore`.
- `src/py/vertebra/db/adapters/valkey/driver.py` — `SyncValkeyDriver` and `AsyncValkeyDriver`.
- `src/py/vertebra/db/adapters/valkey/__init__.py` — Lazy package exports (`PY-06`).
- `src/py/vertebra/_valkey.pyi` — Native `vertebra._valkey` types.

## Contracts & Edge Cases

- Probe `ensure_module("vertebra._valkey", package="vertebra-core[valkey]", extra="valkey")` at driver/core instantiation.
- Accept `valkey://`, `valkeys://`, and `unix://`; reject `memory://`, `redis://`, and `rediss://` with `ImproperConfigurationError`.
- Expose `hexpire`, `hpexpire`, `hexpireat`, `hpexpireat`, `httl`, `hpttl`, and `hpersist` on both `SyncValkeyDriver` and `AsyncValkeyDriver`.
- **Edge Case**: Maintain zero imports from `vertebra.db.adapters.redis` or `vertebra._redis`.

## Test Matrix

- `src/py/tests/unit/db/test_redis_valkey_driver.py` verifies `SyncValkeyDriver` and `AsyncValkeyDriver` delegation to `vertebra._valkey`, Valkey 9.0 hash-field expiration methods, URL scheme validation, and missing-wheel `MissingDependencyError` hints.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/valkey && uv run --no-sync mypy src/py/vertebra/db/adapters/valkey
```

## Acceptance Criteria

- [ ] `SyncValkeyDriver`, `AsyncValkeyDriver`, and `ValkeyCore` delegate to `vertebra._valkey` with zero Python socket/asyncio loops.
- [ ] Zero regressions in the fast verification gate.

[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
[^valkey-cfg]: `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig` — ValkeyConfig.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore RESP3 command builder and hash slot helpers.
