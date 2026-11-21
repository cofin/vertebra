---
type: Task
id: "native-cursor-data-path:cleanup:t16"
chapter_id: cleanup-and-checks
title: "DuckDB secret refresh assertions"
description: "Rewrite the four failing DuckDB secret tests to assert the three refresh points of the contract: connection open, a credential rejection, and an explicit refresh_secrets call."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
  - "src/py/tests/unit/storage/test_gcs_token_refresh.py::test_duckdb_multi_session_token_generation_secret_rotation"
  - "src/py/tests/unit/storage/test_gcs_token_refresh.py::test_async_duckdb_driver_token_generation_rotation"
  - "src/py/tests/unit/storage/test_gcs_token_refresh.py::test_duckdb_driver_401_and_403_invalidates_gcs_bearer_secret_providers"
  - "src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver._on_native_error"
  - "src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver._refresh_dynamic_secrets"
  - "src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver.refresh_secrets"
  - "src/py/vertebra/db/adapters/duckdb/_async.py::AsyncDuckDBDriver._on_native_error"
  - "src/py/vertebra/db/adapters/duckdb/_async.py::AsyncDuckDBDriver.refresh_secrets"
  - "src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore.invalidate_rejected_credentials"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/embedded-cursors/spec.md:198-203"
files: []
tests:
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 50
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: secret-tests
    resource: src/py/tests/unit/storage/test_gcs_token_refresh.py::test_duckdb_driver_401_and_403_invalidates_gcs_bearer_secret_providers
    title: "DuckDB secret rotation and rejection tests"
  - id: duckdb-error-path
    resource: src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver._on_native_error
    title: "Handler that invalidates and refreshes rejected secrets"
  - id: embedded-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/embedded-cursors/spec.md
    title: "DuckDB secret refresh contract"
tags: [testing, database, duckdb, credentials]
---

# t16 — DuckDB secret refresh assertions

## Objective & Scope Boundary

The four failing tests in `test_gcs_token_refresh.py` pass and assert the secret refresh contract from the `embedded-cursors` chapter spec.[^secret-tests][^embedded-spec]

The drivers already meet that contract, so this task edits tests only:

- A successful query calls no provider. Two tests still expect a query to rotate a secret whose provider asked for a refresh.
- On a `401` or `403` rejection, `_on_native_error` invalidates the tracked generation and runs a forced refresh before the error propagates. The rejection test still expects the refresh to wait for the next query.[^duckdb-error-path]

In scope: the three test functions named below, which cover four test cases. Out of scope: every production file and every other test in the file.

## Bounded Read Manifest

- The three test functions.
- `_on_native_error`, `_refresh_dynamic_secrets`, and `refresh_secrets` in the sync driver, and `_on_native_error` and `refresh_secrets` in the async driver.
- `DuckDBCore.invalidate_rejected_credentials` for the pattern `(HTTP 401)` or `(HTTP 403)` and the call to `provider.invalidate`.
- Lines `198-203` of the `embedded-cursors` chapter spec for the contract.

## Contracts & Edge Cases

Each replacement below is exact. Lines not named stay as they are.

### `test_duckdb_multi_session_token_generation_secret_rotation`

Docstring: `Verify DuckDB sessions keep an applied secret across queries and rotate it on refresh_secrets after the provider asks for a refresh.`

Replace the block from `driver_a.select("SELECT 2 AS v")` through `assert provider_b.generation == 2` with:

```python
assert driver_a.select("SELECT 2 AS v") == [{"v": 2}]
assert provider_a.generation == 1
assert provider_a.needs_refresh() is True
assert token_server.oauth_calls == 1

driver_a.refresh_secrets(force=False)
assert provider_a.generation == 2
assert provider_a.needs_refresh() is False
assert token_server.oauth_calls == 2

driver_b.refresh_secrets(force=False)
assert provider_b.generation == 2
```

The two lines after it stay: `assert token_server.oauth_calls == 2` and the `mock-oauth-token-user-2` assertion.

### `test_async_duckdb_driver_token_generation_rotation`

Docstring: `Verify AsyncDuckDBDriver keeps an applied secret across queries and rotates it on refresh_secrets for sync and async providers.`

First driver. Replace `await drv.select("SELECT 1 AS ok")` on the line after `sync_provider.token_cache.get_token(force_refresh=True)` with `await drv.refresh_secrets(force=False)`. The two assertions after it stay.

Second driver. Replace the two lines `await async_drv.select("SELECT 1 AS ok")` and `assert async_provider.generation == 2` that follow `assert async_provider.needs_refresh() is True` with:

```python
assert await async_drv.select("SELECT 1 AS ok") == [{"ok": 1}]
assert async_provider.generation == 1
assert async_provider.needs_refresh() is True
await async_drv.refresh_secrets(force=False)
assert async_provider.generation == 2
```

The two assertions after it stay: `needs_refresh() is False` and the `mock-oauth-token-user-4` assertion.

### `test_duckdb_driver_401_and_403_invalidates_gcs_bearer_secret_providers`

Docstring: `Verify a 401 or 403 rejection invalidates the applied secret and leaves a fresh one applied before the error propagates.`

Sync half. Replace `assert sync_provider.needs_refresh() is True` after the `pytest.raises` block with:

```python
assert sync_provider.needs_refresh() is False
assert sync_provider.generation == 2
```

Async half. Replace `assert async_provider.generation == 1` and `assert async_provider.needs_refresh() is True` after the `pytest.raises` block with:

```python
assert async_provider.generation == 2
assert async_provider.needs_refresh() is False
```

The rest of both halves stays, including the successful `select` after `fail_select = False` and the final `generation == 2` and `needs_refresh() is False` assertions.

Edge cases:

- This task runs before the `single-call-paths` driver tasks. Those tasks own adding the single-call methods to the fault proxies when they route the driver through them.
- A test that still fails after these edits with the driver leaving the old credential in place is a production defect in a method that lost its `_on_native_error` call. Stop and report the method. Do not weaken the assertion.
- No time-based sleep is added. `refresh_secrets(force=False)` applies the secret because the provider asks for a refresh or its generation differs from the applied one.

## Test Matrix

| Test | Expected |
| --- | --- |
| `test_duckdb_multi_session_token_generation_secret_rotation` | Passes; the query leaves generation `1` and one token request; `refresh_secrets` moves to generation `2` with two token requests; the second driver reaches generation `2` with no further request |
| `test_async_duckdb_driver_token_generation_rotation` | Passes; the query leaves generation `1`; `refresh_secrets` moves to generation `2`; the bearer token is `mock-oauth-token-user-4` |
| `test_duckdb_driver_401_and_403_invalidates_gcs_bearer_secret_providers[401]` and `[403]` | Pass; after the rejected call the provider is at generation `2` and does not ask for a refresh, sync and async |
| Every other test in the file | Unchanged result |
| `mypy` and `ruff check` | No error or finding in the file |

## Verification

```bash
uv run --no-sync --frozen pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -p no:xdist -q
uv run --no-sync --frozen pytest src/py/tests/unit/db/test_duckdb_driver.py -p no:xdist -q -k "secret"
uv run --no-sync --frozen ruff check src/py/tests/unit/storage/test_gcs_token_refresh.py
uv run --no-sync --frozen mypy | grep "test_gcs_token_refresh.py"
git diff --stat -- src/py/vertebra src/rs
```

The first two commands report no failure. The `grep` and the last command print nothing.

## Acceptance Criteria

- [ ] The four test cases pass.
- [ ] The rotation tests assert that a query does not rotate a secret and that `refresh_secrets(force=False)` does.
- [ ] The rejection test asserts a fresh credential right after the rejected call.
- [ ] No production file has a diff from this task.

[^secret-tests]: `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_duckdb_driver_401_and_403_invalidates_gcs_bearer_secret_providers`
[^duckdb-error-path]: `src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver._on_native_error`
[^embedded-spec]: `.agents/bundles/specs/native-cursor-data-path/chapters/embedded-cursors/spec.md`
