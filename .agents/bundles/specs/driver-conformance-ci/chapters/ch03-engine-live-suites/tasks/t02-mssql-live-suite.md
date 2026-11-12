---
type: Task
id: "driver-conformance-ci:ch03:t02"
chapter_id: ch03-engine-live-suites
title: Add the live SQL Server edge suite
description: Create a live SQL Server suite covering DATETIMEOFFSET offsets, DECIMAL(38,10), NVARCHAR(MAX) and VARBINARY(MAX) LOBs, WAITFOR cancellation, savepoint rollback, Arrow load and stream, and duplicate-key IntegrityError.
status: draft
state: pending
priority: P1
verification_strategy: integration_acceptance
depends_on:
  - "driver-conformance-ci:ch01:t02"
read_set:
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/db/test_oracle_native.py
  - src/py/vertebra/_vertebra.pyi
files: []
tests:
  - src/py/tests/integration/db/test_mssql_native.py
estimated_loc: 220
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
sources:
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver.cancel
    title: SQL Server cancel, savepoints, load_from_arrow, stream_arrow_batches
  - id: mssql-config
    resource: src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig
    title: SQL Server configuration
  - id: fixtures
    resource: src/py/tests/integration/conftest.py::vtb_mssql_config
    title: Versioned SQL Server container fixture
tags:
  - mssql
  - integration
---

# Task t02: Add the Live SQL Server Edge Suite

## Objective & Scope Boundary

Create `src/py/tests/integration/db/test_mssql_native.py` with `pytestmark = [pytest.mark.driver_engine("mssql")]`, an `mssql_config` fixture, `sync_mssql` / `async_mssql` driver fixtures, a `mode` parametrization where both drivers apply, and the Worked Test Matrix cases [^mssql-driver] [^fixtures].

Resolution: `ensure_module("vertebra._mssql", package="vertebra-core[mssql]", install_package="vertebra-core[mssql]", extra="mssql")` → `VERTEBRA_CONFORMANCE_MSSQL_DSN` → `request.getfixturevalue("vtb_mssql_config")` under `VERTEBRA_CONFORMANCE_DOCKER == "1"` → `pytest.skip("mssql live tests require VERTEBRA_CONFORMANCE_MSSQL_DSN or VERTEBRA_CONFORMANCE_DOCKER=1")`.

Out of scope: production code; failures are recorded as `blocked`.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/mssql/driver.py` — `SyncMSSQLDriver.cancel`, `savepoint`, `rollback_to_savepoint`, `begin`, `commit`, `load_from_arrow`, `stream_arrow_batches`, `AsyncMSSQLDriver` [^mssql-driver].
- `src/py/vertebra/db/adapters/mssql/config.py` — `MSSQLConfig`, `from_url` [^mssql-config].
- `src/py/tests/integration/conftest.py` — `vtb_mssql_config` [^fixtures].
- `src/py/tests/integration/db/test_oracle_native.py` — gate precedent.
- `src/py/vertebra/_vertebra.pyi` — `QueryCancelledError`, `IntegrityError.vendor_code`.

## Symbol & Signature Contracts

```python
pytestmark = [pytest.mark.driver_engine("mssql")]

@pytest.fixture
def mssql_config(request: pytest.FixtureRequest) -> MSSQLConfig: ...

@pytest.fixture
def sync_mssql(mssql_config: MSSQLConfig) -> Iterator[SyncMSSQLDriver]: ...

@pytest.fixture
async def async_mssql(mssql_config: MSSQLConfig) -> AsyncIterator[AsyncMSSQLDriver]: ...
```

- Placeholders use `@P1`, `@P2` (the adapter's `at_upper_p` style).
- Cancellation: driver A runs `WAITFOR DELAY '00:00:30'` in a thread (sync) or task (async); driver B polls `sys.dm_exec_requests WHERE command = 'WAITFOR'` until present; then `driver_a.cancel()` (sync from the main thread; async `await driver_a.cancel()`).

## Edge Cases & Error Semantics

- `DATETIMEOFFSET` keeps the original `+05:30` offset (`utcoffset() == timedelta(hours=5, minutes=30)`), not a UTC-normalized value.
- `NVARCHAR(MAX)` beyond 4,000 characters and `VARBINARY(MAX)` beyond 8,000 bytes exercise PLP streaming.
- Cancellation raises `QueryCancelledError` and leaves the driver usable (`select_value("SELECT 1") == 1`).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_mssql_native.py`. Dependency category: remote-owned SQL Server `2022` / `2025` via `pytest-databases`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `DATETIMEOFFSET` (sync/async) | insert `datetime(2026, 11, 12, 9, 0, 0, 123456, tzinfo=timezone(timedelta(hours=5, minutes=30)))` into `DATETIMEOFFSET(6)` | Equal value; `utcoffset() == timedelta(hours=5, minutes=30)` |
| `DECIMAL(38,10)` | `Decimal("1234567890123456789012345678.0123456789")` | Exact `Decimal` equality |
| `NVARCHAR(MAX)` LOB | `"脊" * 20_000` | Selected `str` equal, `len == 20_000` |
| `VARBINARY(MAX)` LOB | `bytes(range(256)) * 8192` (2 MiB) | Selected `bytes` equal |
| `WAITFOR` cancel (sync/async) | cancel after request observed | `QueryCancelledError` within `10 s`; next `select_value("SELECT 1", value_type=int) == 1` |
| Savepoint rollback | `begin()`; insert `id=1`; `savepoint("sp1")`; insert `id=2`; `rollback_to_savepoint("sp1")`; `commit()` | `select_value("SELECT COUNT(*) FROM vtb_live_sp", value_type=int) == 1` |
| Arrow load + stream | 10,000-row `pa.table` (`id` int64, `label` utf8); `load_from_arrow("vtb_live_arrow", table)`; `stream_arrow_batches(..., batch_size=4096)` | Returns `10_000`; batch row counts `[4096, 4096, 1808]` |
| Duplicate key | insert `id=1` twice | `IntegrityError` with `vendor_code == 2627` |

## Verification

- Offline: `make dev && uv run --no-sync pytest src/py/tests/integration/db/test_mssql_native.py -q -rs` exits `0` with every case skipped.
- Live: `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_ENGINES=mssql VERTEBRA_CONFORMANCE_MSSQL_VERSION=2022 uv run --no-sync pytest src/py/tests/integration/db -q -rs` and the same with `VERTEBRA_CONFORMANCE_MSSQL_VERSION=2025` exit `0`.
- Lint: `uv run --no-sync ruff check src/py/tests/integration/db/test_mssql_native.py && uv run --no-sync ruff format --check src/py/tests/integration/db/test_mssql_native.py`.

## Acceptance Criteria

- [ ] Every Worked Test Matrix row passes on SQL Server `2022` and `2025` (R7).
- [ ] The suite is tagged `driver_engine("mssql")` and skips offline with the exact reason (R7).

## Notes & Discoveries

- [2026-11-12 00:30] SQL Server images are `linux/amd64` only; record this in the limitations record (`ch05 / t04`).

[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver.cancel` — SQL Server driver surface.
[^mssql-config]: `src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig` — SQL Server configuration.
[^fixtures]: `src/py/tests/integration/conftest.py::vtb_mssql_config` — Versioned SQL Server fixture.
