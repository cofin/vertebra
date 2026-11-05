---
type: Task
id: db-engine-primitives:t04
title: Relocate SQL embedding pushdown builders to vertebra/db/adapters/<engine>/embeddings.py and add PushdownEmbedder
description: "Move all database SQL embedding pushdown builders out of `src/py/vertebra/embeddings/` into `src/py/vertebra/db/adapters/{bigquery,spanner,postgres,oracle}/embeddings.py` implementing `EmbeddingPushdown`, add `PushdownEmbedder(Embedder)` in `src/py/vertebra/embeddings/_pushdown.py`, delete `embeddings/_bigquery.py`, `embeddings/_spanner.py`, and `AlloyDbEmbedder`/`OracleEmbedder` in `embeddings/_google.py`, convert `src/py/vertebra/embeddings/__init__.py` to `lazy_exports`, and update embedding documentation and OKF `sources:`."
state: "in_progress"
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - db-engine-primitives:t02
files:
  - .pre-commit-config.yaml
  - src/py/vertebra/db/adapters/bigquery/embeddings.py
  - src/py/vertebra/db/adapters/bigquery/__init__.py
  - src/py/vertebra/db/adapters/spanner/embeddings.py
  - src/py/vertebra/db/adapters/spanner/__init__.py
  - src/py/vertebra/db/adapters/postgres/embeddings.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/db/adapters/oracle/embeddings.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
  - src/py/vertebra/embeddings/_pushdown.py
  - src/py/vertebra/embeddings/_bigquery.py
  - src/py/vertebra/embeddings/_spanner.py
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/embeddings/__init__.py
  - docs/usage/index.md
  - docs/usage/embeddings-and-vectors.md
  - docs/usage/cloud-dialers.md
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
  - .agents/bundles/knowledge/domains/index.md
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/docs/test_examples.py
plan_revision: 6
plan_commit: "954998dea5866f52bc3730db642e8f028d4e72bf"
state_revision: 1
claimed_by: "/root/db_t04"
claimed_at: "2026-11-05T02:10:09Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T21:47:00Z
updated_at: "2026-11-05T02:19:48Z"
commit: null
tags: [db, embeddings, pushdown, bigquery, spanner, postgres, oracle]
---

## Objective

Move all database SQL embedding pushdown builders out of `src/py/vertebra/embeddings/` into `src/py/vertebra/db/adapters/{bigquery,spanner,postgres,oracle}/embeddings.py` implementing `EmbeddingPushdown`, add `PushdownEmbedder(Embedder)` in `src/py/vertebra/embeddings/_pushdown.py`, delete `embeddings/_bigquery.py`, `embeddings/_spanner.py`, and `AlloyDbEmbedder`/`OracleEmbedder` in `embeddings/_google.py`, convert `src/py/vertebra/embeddings/__init__.py` to `lazy_exports`, and update embedding documentation and OKF `sources:`.

## Context

- **Verification strategy (`behavior_tdd`)**: SQL generation, `ORDER BY idx` batch ordering, `validate_identifier` rejection of unsafe SQL identifiers, Oracle 128-byte URL validation and `SyncGcpTokenCache` / `AsyncGcpTokenCache` rotation, `isinstance(..., EmbeddingPushdown)` conformance, and `PushdownEmbedder` inheritance of `Embedder` methods (`embed_query`, `embed_query_sync`, `embed_document`, `embed_document_sync`, `embed_many`, `embed_many_sync`, `embed_arrow`, `similarity_search`, plus `vector_search`) are directly observable in unit tests.
- **Architectural invariants**:
  - `src/py/vertebra/embeddings/_base.py::Embedder` defines `__new__(cls, model="gemini-embedding-001", *, dimensions=768, **kwargs)` and `__init__(self, model="gemini-embedding-001", *, dimensions: int = 768, cache: VectorCache | None = None)` plus `embed_query`, `embed_query_sync`, `embed_document`, `embed_document_sync`, `embed_many`, `embed_many_sync`, `embed_arrow`, and in-memory SIMD `similarity_search` delegating to `_embed_batch_impl(self, texts: list[str], *, task: str = "document", titles: list[str | None] | None = None) -> VectorBatchView`.
  - Therefore `PushdownEmbedder(Embedder)` MUST override `def __new__(cls, *args: Any, **kwargs: Any) -> PushdownEmbedder: return object.__new__(cls)` (so `PushdownEmbedder(pushdown, ...)` never triggers `Embedder.__new__` signature mismatches), accept `(self, pushdown: EmbeddingPushdown, model: str = "gemini-embedding-001", *, dimensions: int = 768, normalize: bool = True, cache: VectorCache | None = None) -> None`, override `async def _embed_single_impl(self, text: str, *, task: str = "query", title: str | None = None) -> VectorView` so `task == "query"` (when `title is None`) calls `await self._pushdown.embed_query(text, dimensions=self._dimensions, normalize=self._normalize)` directly (recording `self._record_usage((text,))` and translating `DataError` into `EmbeddingError`) while delegating document/titled single embeds to `super()._embed_single_impl(...)`, implement `async def _embed_batch_impl(...) -> VectorBatchView` delegating to `await self._pushdown.embed_documents(texts, task=task, dimensions=self._dimensions, normalize=self._normalize)` (translating `DataError` into `EmbeddingError`), and expose `async def vector_search(...) -> list[dict[str, Any]]` delegating to `self._pushdown.similarity_search(...)`.
  - `src/py/vertebra/db/adapters/{bigquery,spanner,postgres,oracle}/embeddings.py` MUST NOT import from `vertebra.embeddings`; import `VectorView` and `VectorBatchView` from `vertebra._vertebra`, raise `DataError` (`vertebra.db.driver`) on empty or malformed vector rows, and execute SQL via `await self._driver.select(...)` / `await self._driver.execute(...)` when `isinstance(self._driver, AsyncDatabaseDriver)` else `await asyncio.to_thread(self._driver.select, ...)` / `await asyncio.to_thread(self._driver.execute, ...)` (replacing nonexistent `driver.fetch_all` calls and avoiding `asyncio.iscoroutine` inspection).
  - Scope identifier validation accurately per dialect (`spec.md` §2.2 & §2.4):
    - `BigQueryEmbeddingPushdown(driver, model="gemini-embedding-001", *, model_endpoint=None, dimensions=768, use_ai_function=False, normalize=True)`: exposes `driver`, `model_endpoint`, `build_generate_embedding_sql()`, and `build_vector_search_sql(...)`; validates `model_endpoint` and `table` with `validate_identifier(..., allow_hyphens=True)` (supporting hyphenated GCP project IDs such as `analytics-proj.analytics.text_embedding_model`) and `embedding_column` with `validate_identifier(..., allow_dots=False)`.
    - `SpannerEmbeddingPushdown(driver, model="gemini-embedding-001", *, model_endpoint=None, dimensions=768, use_ai_function=False, normalize=True, approximate=True, num_leaves_to_search=None)`: exposes `driver`, `model_endpoint` (`model_endpoint or model.replace("-", "_")`), `build_generate_embedding_sql()`, and `build_vector_search_sql(...)`; validates `model_endpoint` and `table` via `validate_identifier` and `embedding_column` with `allow_dots=False`.
    - `PostgresEmbeddingPushdown(driver, model="gemini-embedding-001", *, dimensions=768, normalize=True)`: exposes `driver`, `model`, and `sql_expr(column: str) -> str` (validating `column` via `validate_identifier` and escaping single quotes on non-empty `model`); binds `model` as `$1` in `SELECT embedding($1, t.txt)::vector FROM unnest($2::text[]) WITH ORDINALITY AS t(txt, idx) ORDER BY t.idx` (rejecting empty `model` strings with `ValueError` without rejecting hyphens in `"gemini-embedding-001"`), and validates `table` and `embedding_column` (`allow_dots=False`) in `similarity_search`.
    - `OracleEmbeddingPushdown(driver, model="gemini-embedding-001", *, dimensions=768, provider="vertexai", project=None, project_number=None, location="us-central1", credential_name="VERTEX_CRED", endpoint_url=None, token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None, normalize=True)`: exposes `ORACLE_URL_MAX_LEN = 128`, `driver`, `resolved_model_parameter`, `endpoint_url`, and `applied_token_generation`; validates `credential_name` (`allow_dots=False`), `table`, and `embedding_column` (`allow_dots=False`) via `validate_identifier`, passes `resolved_model_parameter` (`"gemini-embedding-001:predict"`) inside `JSON(:params)` rather than `validate_identifier`, and dispatches `DROP_CREDENTIAL` / `CREATE_CREDENTIAL` PL/SQL in `_ensure_oracle_credential_fresh` across both sync and async token caches (`SyncGcpTokenCache` / `AsyncGcpTokenCache`) and sync and async drivers via `isinstance(self._driver, AsyncDatabaseDriver)` vs `await asyncio.to_thread(self._driver.execute, ...)`.
  - Clean break: leave zero `BigQueryEmbedder`, `SpannerEmbedder`, `AlloyDbEmbedder`, or `OracleEmbedder` aliases in `vertebra.embeddings`.

## Steps

1. **Write failing `EmbeddingPushdown` adapter, `PushdownEmbedder`, and clean-break tests (RED)**:
   - Update `src/py/tests/unit/embeddings/test_google_embedders.py`, `src/py/tests/unit/db/test_bigquery.py`, and `src/py/tests/unit/db/test_spanner.py` using `@pytest.mark.parametrize`:
     - Assert `BigQueryEmbeddingPushdown` (including hyphenated `analytics-proj.analytics.text_embedding_model`), `SpannerEmbeddingPushdown`, `PostgresEmbeddingPushdown` (covering `sql_expr`, AlloyDB `embedding($1, t.txt)::vector` with `model="gemini-embedding-001"` and `ORDER BY t.idx`, and `pgvector` distance operators `<=>`, `<->`, `<#>`), and `OracleEmbeddingPushdown` (covering `DBMS_VECTOR.UTL_TO_EMBEDDING`, `resolved_model_parameter == "gemini-embedding-001:predict"`, 128-byte UTF-8 provider URL check, and `SyncGcpTokenCache` / `AsyncGcpTokenCache` rotation across sync and async drivers) each satisfy `isinstance(..., EmbeddingPushdown)`, dispatch through `driver.select(...)` across sync and async drivers, raise `DataError` on empty vector rows, and reject unsafe SQL identifiers via `validate_identifier` with `ValueError`.
     - Assert `PushdownEmbedder(pushdown, model="gemini-embedding-001", dimensions=4)` routes `embed_query` / `embed_query_sync` through `pushdown.embed_query(...)` and `embed_document` / `embed_document_sync` / `embed_many` / `embed_many_sync` / `embed_arrow` through `pushdown.embed_documents(...)`, supports in-memory `similarity_search` and `vector_search`, populates `VectorCache` on cache misses, and raises `EmbeddingError` when the pushdown adapter raises `DataError` for an empty vector row.
     - Assert `hasattr(vertebra.embeddings, name) is False` for `BigQueryEmbedder`, `SpannerEmbedder`, `AlloyDbEmbedder`, and `OracleEmbedder`, and `import vertebra.embeddings` in a clean `sys.modules` state loads submodules lazily via `lazy_exports`.

2. **Implement adapter `embeddings.py` modules, `PushdownEmbedder`, `embeddings/__init__.py` lazy exports, and doc/OKF updates (GREEN)**:
   - Create:
     - `src/py/vertebra/db/adapters/bigquery/embeddings.py` (`BigQueryEmbeddingPushdown`) and re-export in `adapters/bigquery/__init__.py`.
     - `src/py/vertebra/db/adapters/spanner/embeddings.py` (`SpannerEmbeddingPushdown`) and re-export in `adapters/spanner/__init__.py`.
     - `src/py/vertebra/db/adapters/postgres/embeddings.py` (`PostgresEmbeddingPushdown`) and re-export in `adapters/postgres/__init__.py`.
     - `src/py/vertebra/db/adapters/oracle/embeddings.py` (`OracleEmbeddingPushdown`) and re-export in `adapters/oracle/__init__.py`.
   - Create `src/py/vertebra/embeddings/_pushdown.py` (`PushdownEmbedder(Embedder)`).
   - Delete `src/py/vertebra/embeddings/_bigquery.py` and `src/py/vertebra/embeddings/_spanner.py` (`git rm`), and delete `AlloyDbEmbedder` and `OracleEmbedder` (plus their SQL/URL helpers) from `src/py/vertebra/embeddings/_google.py`, keeping only `VertexEmbedder`, `GeminiEmbedder`, and `GrpcTransportCallable`.
   - Convert `src/py/vertebra/embeddings/__init__.py` to `if TYPE_CHECKING:` imports + `lazy_exports(__name__, __all__, _EXPORT_MAP)` exporting `PushdownEmbedder` alongside the base, cloud, local, remote, model-catalog, and broker symbols.
   - Update `docs/usage/index.md`, `docs/usage/embeddings-and-vectors.md`, `docs/usage/cloud-dialers.md`, `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` (removing deleted `_bigquery.py` / `_spanner.py` from `sources:`, adding `_pushdown.py` and `db/adapters/{bigquery,spanner,postgres,oracle}/embeddings.py`, and keeping the `<!-- truth: start -->` block `<= 40` lines), and `.agents/bundles/knowledge/domains/index.md`.

3. **Refactor & verify**:
   - Run `uv run pytest src/py/tests/unit/embeddings/ src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/docs/test_examples.py` and `make lint-all`.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/embeddings/test_google_embedders.py` -> fails (`ImportError: cannot import name 'PushdownEmbedder'`).
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit/embeddings/ src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/docs/test_examples.py` -> exits `0`.
  - `make lint-all` -> exits `0`.

## Acceptance Criteria

- [x] R4: `BigQueryEmbeddingPushdown`, `SpannerEmbeddingPushdown`, `PostgresEmbeddingPushdown`, and `OracleEmbeddingPushdown` satisfy `EmbeddingPushdown`, execute via `driver.select(...)`, validate identifiers via `validate_identifier`, and contain zero imports from `vertebra.embeddings`.
- [x] R4: `PushdownEmbedder` inherits `Embedder`'s `embed_*` / `embed_arrow` / `similarity_search` methods and exposes `vector_search`, `embeddings/{_bigquery,_spanner}.py` and `AlloyDbEmbedder`/`OracleEmbedder` are deleted with zero aliases, `vertebra.embeddings` uses `lazy_exports`, and `make lint-all` (`make review-check`) exits `0`.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->

- [2026-11-05T02:33:23+00:00] Behavior-TDD RED: missing adapter-owned embedding capability. GREEN: embeddings/BQ/Spanner/docs selection155 passed; default Vertex factory and base class unchanged; mapping/tuple/vector validation covered; 12-module Mypy and focused Ruff clean. Combined make lint-all and make docs exit0.

- [2026-11-05] Commit spelling validation flags the existing HTTP/2 `"te": "trailers"` header in the retained Vertex code. Add a narrowly matched spelling exception for that exact protocol header, preserving other spelling checks. An isolated valid-header probe passes and an unrelated spelling violation still fails. The protocol header and runtime behavior remain unchanged.
