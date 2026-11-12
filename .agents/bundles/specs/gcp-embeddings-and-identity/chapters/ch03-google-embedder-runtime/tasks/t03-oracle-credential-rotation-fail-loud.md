---
type: Task
id: "gcp-embeddings-and-identity:ch03:t03"
chapter_id: ch03-google-embedder-runtime
title: Rotate Oracle Vertex credentials only from an explicit token cache
description: Remove the implicit metadata-server token cache, storage fallback, and silent ValueError skip from OracleEmbeddingPushdown, convert SyncGcpTokenCache to async, and propagate token errors.
status: draft
state: pending
priority: P2
verification_strategy: regression_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch03:t02"
read_set:
  - src/py/vertebra/db/adapters/oracle/embeddings.py
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - src/py/vertebra/_vertebra.pyi
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch03-google-embedder-runtime/spec.md
files:
  - src/py/vertebra/db/adapters/oracle/embeddings.py
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
estimated_loc: 70
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
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
commit: null
sources:
  - id: py-oracle-pushdown
    resource: src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown._ensure_oracle_credential_fresh
    title: Implicit metadata cache, storage fallback, and silent ValueError return
  - id: py-oracle-test
    resource: src/py/tests/unit/embeddings/test_google_embedders.py::test_oracle_url_bytes_and_token_rotation
    title: Existing rotation test with sync and async duck-typed token ports
tags:
  - oracle
  - embeddings
  - fail-loud
---

# Task t03: Rotate Oracle Vertex Credentials Only from an Explicit Token Cache

## Objective & Scope Boundary

Change `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown` so credential rotation is explicit [^py-oracle-pushdown]:

- `__init__` stores `token_cache.to_async()` for `SyncGcpTokenCache` instances and other objects as given.
- `_ensure_oracle_credential_fresh` returns immediately when `self._token_cache is None` (the caller manages `DBMS_VECTOR` credentials, for example a DBA-created `VERTEX_CRED`).
- Remove the implicit `AsyncGcpTokenCache({"use_metadata_server": "true"})` construction, the `import_optional("vertebra.storage._store")` fallback, and the `except ValueError: return`; remove the `import_optional` import if unused.
- Token errors from `get_token` propagate to the caller.

Out of scope: SQL text, URL validation, vector decoding.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown.__init__`, `::_ensure_oracle_credential_fresh`, imports.
- `src/py/tests/unit/embeddings/test_google_embedders.py::test_oracle_url_bytes_and_token_rotation`, `::_SelectPort`, `::_AsyncSelectPort` [^py-oracle-test].
- `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache.to_async`.
- Chapter spec.

## Symbol & Signature Contracts

```python
class OracleEmbeddingPushdown:
    def __init__(self, driver: AsyncOracleDriver | SyncOracleDriver, model: str = "gemini-embedding-001", *,
                 dimensions: int = 768, provider: str = "vertexai", project: str | None = None,
                 project_number: str | None = None, location: str = "us-central1",
                 credential_name: str = "VERTEX_CRED", endpoint_url: str | None = None,
                 token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
                 normalize: bool = True) -> None: ...      # signature unchanged
    async def _ensure_oracle_credential_fresh(self) -> None: ...
```

The method docstring states that rotation runs only when a token cache is supplied.

## Edge Cases & Error Semantics

- `token_cache=None`: zero `DROP_CREDENTIAL` / `CREATE_CREDENTIAL` statements.
- Duck-typed sync token ports (tests) still work: non-awaitable `get_token()` results are used directly.
- A `get_token` exception propagates from `embed_query` / `embed_documents` before any inference SQL runs.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/embeddings/test_google_embedders.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `test_oracle_url_bytes_and_token_rotation` (existing, parametrized over sync/async ports and drivers) | Unchanged | Still passes |
| `test_oracle_without_token_cache_skips_rotation` | `@pytest.mark.parametrize("port_type", [_SelectPort, _AsyncSelectPort])`; `OracleEmbeddingPushdown(port, project_number="123", dimensions=4)`; `await pushdown.embed_query("q")` | `0` SQL statements containing `CREATE_CREDENTIAL` or `DROP_CREDENTIAL`; one inference statement |
| `test_oracle_token_cache_errors_propagate` | Token port whose `get_token` raises `RuntimeError("token denied")` | `pytest.raises(RuntimeError, match="token denied")`; no inference SQL recorded |
| `test_oracle_sync_native_cache_converted` | `token_cache=SyncGcpTokenCache({"access_token": "tok"}, [])` | `isinstance(pushdown._token_cache, AsyncGcpTokenCache)` |

## Verification

```bash
uv run pytest src/py/tests/unit/embeddings/test_google_embedders.py -q -k oracle
uv run ruff check src/py/vertebra/db/adapters/oracle/embeddings.py src/py/tests/unit/embeddings/test_google_embedders.py
uv run mypy src/py/vertebra/db/adapters/oracle/embeddings.py
```

Commit as `fix(oracle): rotate Vertex credentials only from explicit token caches`.

## Acceptance Criteria

- [ ] No implicit token cache, storage fallback, or silent skip remains (R10).
- [ ] Token errors propagate; `token_cache=None` issues no credential SQL (R10).

## Notes & Discoveries

- [2026-11-12 00:35] Users who relied on implicit ADC rotation now pass `token_cache=AsyncGcpTokenCache({}, ["https://www.googleapis.com/auth/cloud-platform"])`; `ch06:t01` documents this.

[^py-oracle-pushdown]: `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown._ensure_oracle_credential_fresh` — Rotation path.
[^py-oracle-test]: `src/py/tests/unit/embeddings/test_google_embedders.py::test_oracle_url_bytes_and_token_rotation` — Existing rotation test.
