---
type: Task
id: "gcp-embeddings-and-identity:ch03:t01"
flow_id: gcp-embeddings-and-identity
chapter_id: ch03-pushdown-and-local-embedding-broker
title: "Make OracleEmbeddingPushdown Credential Rotation Explicit and Fail-Loud"
description: "Rotate Oracle DBMS_VECTOR credentials only when token_cache is explicitly passed, normalize SyncGcpTokenCache via to_async(), propagate token errors, and enforce exact vector dimensions."
status: stable
state: completed
commit: 5771c8e3
last_verified_commit: 5771c8e3
last_verified_at: "2026-11-13T05:20:15Z"
verification_evidence: "ruff + mypy (12 files) + pytest (404 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch01:t04"
files:
  - src/py/vertebra/db/adapters/oracle/embeddings.py
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - src/py/tests/unit/db/test_oracle_adapter.py
read_set:
  - src/py/vertebra/db/adapters/oracle/embeddings.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 95
tags:
  - oracle
  - embeddings
  - pushdown
  - gcp
sources:
  - id: py-oracle-pushdown
    resource: src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown._ensure_oracle_credential_fresh
    title: "Oracle credential rotation with implicit metadata cache and silent ValueError skip"
---

# Task `t01`: Make `OracleEmbeddingPushdown` Credential Rotation Explicit and Fail-Loud

## Objective & Scope Boundary

Harden `OracleEmbeddingPushdown` in `src/py/vertebra/db/adapters/oracle/embeddings.py` so credential rotation runs strictly from an explicit `token_cache`, normalizes `SyncGcpTokenCache` via `.to_async()`, propagates refresh errors, and validates `top_k > 0` and exact `len(cell) == dimensions` per [`../spec.md` §2](../spec.md) [^py-oracle-pushdown]. Out of scope: BigQuery, Spanner, and PostgreSQL pushdowns (`t02`).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/oracle/embeddings.py` — `__init__`, `_ensure_oracle_credential_fresh`, `similarity_search`, and `_extract_vector` [^py-oracle-pushdown].
- `src/py/vertebra/_vertebra.pyi` — `SyncGcpTokenCache` and `AsyncGcpTokenCache`.
- `src/py/tests/unit/embeddings/test_google_embedders.py` — Oracle pushdown unit tests.
- `src/py/tests/unit/db/test_oracle_adapter.py` — Oracle adapter pushdown tests.

## Contracts & Edge Cases

- Delete implicit `AsyncGcpTokenCache({"use_metadata_server": "true"})` construction, `import_optional("vertebra.storage._store")`, and silent `except ValueError: return` in `_ensure_oracle_credential_fresh`.
- When `token_cache is None`, execute zero `DROP_CREDENTIAL` / `CREATE_CREDENTIAL` statements; when `isinstance(token_cache, SyncGcpTokenCache)`, store `token_cache.to_async()` at `__init__` (while supporting duck-typed test caches); propagate any `get_token()` exception unmodified.
- Raise `ValueError("top_k must be positive")` when `top_k <= 0` in `similarity_search`, and raise `DataError` in `_extract_vector` when `len(cell) != dimensions` (never silently slicing oversized vectors).

## Test Matrix

- Parametrized tests in `test_google_embedders.py` and `test_oracle_adapter.py`: `token_cache=None` issues only `SELECT DBMS_VECTOR.UTL_TO_EMBEDDINGS(...)`; explicit `SyncGcpTokenCache` converts via `.to_async()` and rotates only on generation advance; `get_token()` raising `RuntimeError` / `GoogleCloudAuthError` propagates from `embed_query`; `top_k <= 0` raises `ValueError`; oversized vector row (`len > dimensions`) raises `DataError`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/adapters/oracle/embeddings.py && uv run --no-sync mypy src/py/vertebra/db/adapters/oracle/embeddings.py && uv run --no-sync pytest src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/db/test_oracle_adapter.py -k oracle -q
```

## Acceptance Criteria

- [x] `OracleEmbeddingPushdown` never constructs implicit token caches or imports `vertebra.storage`, propagates token refresh failures, and validates `top_k` and exact `dimensions` (`R10`).
- [x] Zero regressions in Oracle embedding pushdown tests.

[^py-oracle-pushdown]: `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown._ensure_oracle_credential_fresh` — Oracle credential rotation with implicit metadata cache and silent `ValueError` skip.
