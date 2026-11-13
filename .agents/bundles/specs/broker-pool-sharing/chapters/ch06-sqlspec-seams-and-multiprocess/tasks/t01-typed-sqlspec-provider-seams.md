---
type: Task
id: "broker-pool-sharing:ch06:t01"
flow_id: broker-pool-sharing
chapter_id: ch06-sqlspec-seams-and-multiprocess
title: "Typed Sync and Async SQLSpec Provider Protocols and Fail-Loud Quarantine"
description: "Remove inspect.signature and contextlib.suppress(Exception) in provider.py; quarantine leases on failed release."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch05:t01"
  - "broker-pool-sharing:ch05:t04"
files:
  - src/py/vertebra/extensions/sqlspec/provider.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/provider.py
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/errors.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
estimated_loc: 215
tags:
  - sqlspec
  - quarantine
  - python
sources:
  - id: sqlspec-provider
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider
    title: "Broker-side SQLSpec provider session and lease management"
---

# Task `t01`: Typed Sync and Async SQLSpec Provider Protocols and Fail-Loud Quarantine

## Objective & Scope Boundary

Replace signature reflection and suppressed release errors across sync and async paths in `provider.py` with `SyncBrokerLeaseSource`, `AsyncBrokerLeaseSource`, `SyncLeasedSession`, and `AsyncLeasedSession` per [`../spec.md` §2](../spec.md) ([#98](https://github.com/cofin/vertebra/issues/98)) [^sqlspec-provider].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/provider.py` — `_call_broker_acquire`, `_call_with_params`, and `_release_*` methods.
- `src/py/vertebra/extensions/broker/transport.py` — `SyncConnectionLease` and `AsyncConnectionLease`.
- `src/py/vertebra/extensions/broker/errors.py` — `LeaseQuarantinedError`.
- `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` — SQLSpec provider unit tests.

## Contracts & Edge Cases

- Delete `_call_broker_acquire`, `_call_with_params`, and every `contextlib.suppress(Exception)` wrapper in `_release_session_sync`, `_release_session_async`, `_release_broker_lease`, and `_release_broker_lease_async`.
- On rollback or exit exception, invoke `broker.quarantine(lease)` and `raise LeaseQuarantinedError(...) from exc` without calling `lease.release()`.
- **Edge Case**: Parameter dispatch handles `None`, `()`, `Mapping`, and `Sequence` deterministically without signature inspection.
- **Edge Case**: If `quarantine` itself raises, propagate with the rollback error preserved on `__context__`.

## Test Matrix

- `@pytest.mark.parametrize` over sync/async rollback failures and `__exit__`/`__aexit__` failures asserts `quarantine` count == 1, `release` count == 0, and chained `LeaseQuarantinedError`.
- Confirm zero `inspect.signature` calls (`PY-10`).

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/provider.py && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/provider.py
```

## Acceptance Criteria

- [ ] Zero `inspect.signature` or `contextlib.suppress(Exception)` remain in `provider.py`.
- [ ] Uncertain sync and async session releases always quarantine and raise `LeaseQuarantinedError`.
- [ ] Zero regressions in the chapter fast verification gate.

[^sqlspec-provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider` — Broker-side SQLSpec provider session and lease management.
