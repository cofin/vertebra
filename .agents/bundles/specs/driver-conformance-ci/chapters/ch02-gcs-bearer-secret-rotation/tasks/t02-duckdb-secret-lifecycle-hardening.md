---
type: Task
id: "driver-conformance-ci:ch02:t02"
chapter_id: ch02-gcs-bearer-secret-rotation
title: Harden DuckDB secret validation and invalidate providers on credential rejection
description: Reject persistent dynamic secrets and invalid refresh intervals in DuckDBSecretConfig, and make SyncDuckDBDriver and AsyncDuckDBDriver invalidate credential providers when a statement fails with an HTTP 401 or 403 rejection before re-raising.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "driver-conformance-ci:ch02:t01"
read_set:
  - src/py/vertebra/db/adapters/duckdb/config.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/tests/unit/db/test_duckdb_driver.py
  - .agents/bundles/specs/driver-conformance-ci/chapters/ch02-gcs-bearer-secret-rotation/spec.md
files:
  - src/py/vertebra/db/adapters/duckdb/config.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
estimated_loc: 200
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
  - id: secret-config
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig.effective_refresh_interval
    title: Refresh interval clamping and CREATE SECRET rendering
  - id: sync-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver.execute
    title: Sync driver statement entry points calling _refresh_dynamic_secrets
  - id: async-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::AsyncDuckDBDriver._refresh_dynamic_secrets_async
    title: Async refresh with generation bookkeeping
  - id: duckdb-tests
    resource: src/py/tests/unit/db/test_duckdb_driver.py::test_duckdb_arrow_c_stream_ipc_bytes_and_dynamic_secret_provider_rotation
    title: Existing DuckDB dynamic secret tests
tags:
  - duckdb
  - secrets
  - fail-loud
---

# Task t02: Harden DuckDB Secret Validation and Invalidate Providers on Credential Rejection

## Objective & Scope Boundary

1. `src/py/vertebra/db/adapters/duckdb/config.py`: add `DuckDBSecretConfig.__post_init__` rejecting `persistent=True` together with a `value_provider`, and `refresh_interval_seconds` that is negative, `NaN`, or infinite; `effective_refresh_interval` returns `float(self.refresh_interval_seconds)` without clamping [^secret-config].
2. `src/py/vertebra/db/adapters/duckdb/driver.py`: add module constant `_CREDENTIAL_REJECTION_PATTERN` and a private `_invalidate_rejected_credentials(self, exc: BaseException) -> None` on both `SyncDuckDBDriver` and `AsyncDuckDBDriver`; wrap every public statement entry point that calls `_refresh_dynamic_secrets()` / `_refresh_dynamic_secrets_async()` (the `execute`, `execute_many`, `select*`, `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` paths) so a raised `DatabaseError` passes through `_invalidate_rejected_credentials` and is re-raised unchanged [^sync-driver] [^async-driver].
3. Add canonical tests in `src/py/tests/unit/db/test_duckdb_driver.py` [^duckdb-tests].

Out of scope: GCS provider classes (`t01`), live `httpfs` (`t03`), automatic retry.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/duckdb/config.py` — `DuckDBSecretConfig`, `effective_refresh_interval`, `to_create_secret_sql` [^secret-config].
- `src/py/vertebra/db/adapters/duckdb/driver.py` — `SyncDuckDBDriver` statement methods, `_refresh_dynamic_secrets`, `AsyncDuckDBDriver` counterparts, `_probe_provider_generation` [^sync-driver] [^async-driver].
- `src/py/tests/unit/db/test_duckdb_driver.py` — existing dynamic secret tests and helpers [^duckdb-tests].
- Chapter spec edge-case matrix.

## Symbol & Signature Contracts

```python
class DuckDBSecretConfig(FrozenBaseStruct, frozen=True, gc=True):
    def __post_init__(self) -> None:
        """Reject persistent dynamic secrets and non-finite or negative refresh intervals.

        Raises:
            ImproperConfigurationError: When `persistent` is combined with `value_provider`, or when
                `refresh_interval_seconds` is negative, NaN, or infinite.
        """

_CREDENTIAL_REJECTION_PATTERN: Final[re.Pattern[str]] = re.compile(r"\(HTTP (?:401|403)\)")

class SyncDuckDBDriver:
    def _invalidate_rejected_credentials(self, exc: BaseException) -> None:
        """Invalidate dynamic secret providers when `exc` reports an HTTP 401 or 403 credential rejection."""

class AsyncDuckDBDriver:
    def _invalidate_rejected_credentials(self, exc: BaseException) -> None: ...
```

- Messages: `f"DuckDB secret {self.name!r} cannot combine persistent=True with value_provider; rotated credentials stay in memory"` and `f"DuckDB secret {self.name!r} refresh_interval_seconds must be a finite number >= 0"`.
- `_invalidate_rejected_credentials`: returns when `_CREDENTIAL_REJECTION_PATTERN.search(str(exc))` is `None`; otherwise for each secret in `self._dynamic_secrets` whose `value_provider` (or its `__self__`) exposes a callable `invalidate` and whose `self._secret_generations.get(secret.name.lower())` is not `None`, calls `invalidate(applied_gen)` once. Invalidation errors are chained: an exception raised by `invalidate` is attached as `__context__` by re-raising the original `exc` from the wrapper (the wrapper always re-raises the original statement error).
- Wrapper shape (sync): `try: ... except DatabaseError as exc: self._invalidate_rejected_credentials(exc); raise`. The async driver uses the same synchronous helper (`invalidate` is synchronous on both token caches).
- No new module-level helper functions beyond the constant; the helper is a private method placed below public methods.

## Edge Cases & Error Semantics

- `refresh_interval_seconds=0.0` stays valid (refresh before every statement).
- `persistent=True` with static `value` stays valid.
- `(HTTP 404)` or `Parser Error` text: no invalidation.
- Provider lacking `invalidate`, or secret never applied (no generation recorded): skipped.
- The original `DatabaseError` instance (same `id`) propagates.
- Mapping-form secrets (`{"name": ..., "persistent": True, "value_provider": f}`) normalized by `DuckDBSecretConfig.from_mapping` raise the same `ImproperConfigurationError` when `DuckDBConfig.normalized_secrets()` runs.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_duckdb_driver.py`. Dependency category: in-process embedded DuckDB (`vertebra._duckdb`, guarded by the module's existing `pytest.importorskip` pattern) with a test-local provider double exposing `generation`, `invalidate`, and `__call__` (port double at the `value_provider` seam). DuckDB's `error(msg)` scalar raises a deterministic error carrying the message text.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Persistent + provider | `DuckDBSecretConfig(name="gcs_lake", secret_type="gcs", persistent=True, value_provider=lambda: {"bearer_token": "t"})` | `ImproperConfigurationError` matching `cannot combine persistent=True with value_provider` |
| Invalid intervals | parametrize `refresh_interval_seconds` over `(-1.0, float("nan"), float("inf"))` | `ImproperConfigurationError` matching `must be a finite number >= 0` |
| Zero interval valid | `refresh_interval_seconds=0.0` | `effective_refresh_interval() == 0.0` |
| Static persistent valid | `persistent=True, value={"key_id": "k", "secret": "s"}` | `to_create_secret_sql()` starts with `CREATE OR REPLACE PERSISTENT SECRET gcs_lake` |
| Sync 401 invalidates | driver with provider (`generation=1`); `driver.select("SELECT error('HTTP GET error on ''gs://b/x'' (HTTP 401)')")` | Raises `DatabaseError` whose text contains `(HTTP 401)`; `provider.invalidated == [1]` |
| Sync 403 invalidates | same with `(HTTP 403)` via `driver.execute(...)` | `provider.invalidated == [1]` |
| Non-credential error | `select("SELECT error('HTTP GET error (HTTP 404)')")` | Raises; `provider.invalidated == []` |
| Async 401 invalidates | `AsyncDuckDBDriver` with async provider; `await driver.select_value("SELECT error('(HTTP 401)')")` | Raises; `provider.invalidated == [1]` |
| Next statement rotates | after the 401 case provider bumps `generation` to `2` inside `invalidate`; `driver.select("SELECT 1 AS v")` | `[{"v": 1}]`; provider `calls == 2` |

## Verification

- Red: `uv run --no-sync pytest src/py/tests/unit/db/test_duckdb_driver.py -k "persistent or interval or credential" -q` fails.
- Green: `make dev && uv run --no-sync pytest src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/unit/storage/test_gcs_token_refresh.py -q`.
- Lint/type: `uv run --no-sync ruff check src/py/vertebra/db/adapters/duckdb src/py/tests/unit/db/test_duckdb_driver.py && uv run --no-sync mypy src/py/vertebra/db/adapters/duckdb`.
- Cloud-agnostic check: `! git grep -n -i "gcs\|gcp\|vertebra.storage" -- src/py/vertebra/db/adapters/duckdb/driver.py`.

## Acceptance Criteria

- [ ] `DuckDBSecretConfig` rejects persistent dynamic secrets and invalid intervals with exact messages (R12).
- [ ] Both drivers invalidate providers on `(HTTP 401)` / `(HTTP 403)` and re-raise the original error without retry (R13).
- [ ] The DuckDB adapter stays free of GCP and storage imports (R13).

## Notes & Discoveries

- [2026-11-12 00:30] Confirm the live `httpfs` rejection text in `t03`; if DuckDB formats the status differently, update `_CREDENTIAL_REJECTION_PATTERN` and the limitations record together.

[^secret-config]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig.effective_refresh_interval` — Interval clamping and DDL rendering.
[^sync-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver.execute` — Sync statement entry points.
[^async-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::AsyncDuckDBDriver._refresh_dynamic_secrets_async` — Async refresh bookkeeping.
[^duckdb-tests]: `src/py/tests/unit/db/test_duckdb_driver.py::test_duckdb_arrow_c_stream_ipc_bytes_and_dynamic_secret_provider_rotation` — Existing DuckDB dynamic secret tests.
