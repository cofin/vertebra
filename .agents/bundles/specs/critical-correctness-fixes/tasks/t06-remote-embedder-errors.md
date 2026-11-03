---
type: Task
id: critical-correctness-fixes:t06
title: Remote embedder error propagation and identifier validation
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
files:
  - src/py/vertebra/embeddings/_base.py
  - src/py/vertebra/embeddings/__init__.py
  - src/py/vertebra/embeddings/_spanner.py
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/embeddings/_bigquery.py
  - docs/usage/embeddings-and-vectors.md
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
  - docs/changelog/index.md
tests:
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/embeddings/test_google_embedders.py
plan_revision: 2
plan_commit: 24e2931
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-03T17:01:41Z
last_verified_commit: 78ffdf6
verification_evidence: "RED: 3 collection ImportErrors (then 12 failed, 18 passed); GREEN: 41 passed in 3.18s; ruff check/format and mypy passed on 9 source files"
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T17:01:41Z
commit: null
tags: [bugfix, embeddings, spanner, oracle, bigquery]
---

# t06 — Remote embedder error propagation and identifier validation

## Objective

In-database embedders return real vectors or raise: `SpannerEmbedder` propagates driver errors and rejects incomplete results instead of substituting hash vectors, `OracleEmbedder` raises a typed error on an empty result, and the BigQuery/Spanner SQL builders validate every interpolated identifier.

## Context

- **Strategy**: `regression_tdd`. Every defect reproduces at `142cc49`.
- **Defects confirmed at HEAD**:
  - `src/py/vertebra/embeddings/_spanner.py::SpannerEmbedder._embed_batch_impl` wraps the driver call in `except Exception: rows = []` and fills missing or vector-less rows from `_extract_vector(..., fallback_text=...)`, which returns `_deterministic_vector` (SHA-256 noise). Rows are matched by index with no length check.
  - `src/py/vertebra/embeddings/_google.py::OracleEmbedder._embed_batch_impl` reads `rows[0]`, which raises `IndexError` when `fetch_all` returns `[]`.
  - `src/py/vertebra/embeddings/_bigquery.py::BigQueryEmbedder.build_generate_embedding_sql` interpolates ``MODEL `{self._model_endpoint}` ``; `build_vector_search_sql` interpolates `base_table`, `'{embedding_column}'`, and `'{dist}'` unvalidated. `SpannerEmbedder.build_generate_embedding_sql` interpolates `MODEL {self._model_endpoint}`, and `build_vector_search_sql` interpolates `base_table` and `embedding_column`.
  - Both `build_generate_embedding_sql` builders put `ORDER BY idx` inside the input subquery (`(SELECT content FROM UNNEST(@texts) AS content WITH OFFSET AS idx ORDER BY idx)`) and drop `idx` from the output. A subquery `ORDER BY` does not fix the order of the `ML.PREDICT` / `ML.GENERATE_EMBEDDING` output, so rows can come back in a different order than the texts while the embedders match them by position.
  - `BigQueryEmbedder._embed_batch_impl` has no row-count check, and `_extract_embedding_from_row` returns `[]` for a row without a vector, so a short or vector-less result yields fewer or empty vectors instead of an error.
- **Existing test that changes**: `src/py/tests/unit/db/test_spanner.py::test_async_spanner_driver_and_embedder_and_datafusion` calls `embedder.embed_many(...)` against `FakeSpannerClient` (SQLite), which cannot run `ML.PREDICT`; it passes today only because the error is swallowed and replaced with hash vectors. Its two embedding lines are removed; the stub-driver tests below cover embedding behavior.
- **Decisions** (spec §2.2):
  - New `EmbeddingError(VertebraError, RuntimeError)` in `src/py/vertebra/embeddings/_base.py`, exported from `vertebra.embeddings`. Subclassing `RuntimeError` keeps existing `except RuntimeError` handlers (the embedders already raise `RuntimeError` for transport failures) working.
  - Driver exceptions propagate unchanged (no wrapping).
  - Identifier rules: BigQuery model endpoint and table use `validate_identifier(..., allow_hyphens=True)` (project IDs contain hyphens); Spanner model endpoint and table use `validate_identifier(...)` (dots allowed, no hyphens; the default endpoint already replaces `-` with `_`); embedding columns use `allow_dots=False`. Unknown distance types raise `ValueError` in both builders.
  - Vectors shorter than `dimensions` keep the existing zero padding; only missing vectors raise.
  - Ordering: both generate builders pass `idx` through the model function (`ML.PREDICT`, `ML.GENERATE_EMBEDDING`, and `AI.GENERATE_EMBEDDING` return their input columns alongside the output) and order the outer query by it while keeping the vector in column 0: `SELECT <vector> AS embedding, idx FROM <fn>(MODEL ..., (SELECT content, idx FROM UNNEST(@texts) AS content WITH OFFSET AS idx)[, STRUCT(...)]) ORDER BY idx`. Column 0 stays the vector because `BigQueryEmbedder._execute_driver_rows` (`fetch_all` tuple rows) and `_extract_embedding_from_row` read `row[0]`. The embedders keep matching rows by position, which the outer `ORDER BY` makes correct.
- **Canonical test modules**: `src/py/tests/unit/db/test_spanner.py` (holds the `SpannerEmbedder` test and imports `SpannerEmbedder`), `src/py/tests/unit/db/test_bigquery.py` (holds the `BigQueryEmbedder` test), `src/py/tests/unit/embeddings/test_google_embedders.py` (holds the `OracleEmbedder` test and `_FakeTokenCache`).
- **Code rules**: `msgspec.Struct(frozen=True, gc=False)` instead of dataclasses; PEP 585/604/695 types; Google-style docstrings; zero inline `#` comments; top-level imports only (except `TYPE_CHECKING` and optional-dependency helpers); class layout attributes → `__init__` → properties → public methods → private methods; JSON through `vertebra.utils.serializers`; `@pytest.mark.parametrize` instead of loops over cases; no compatibility aliases or `try/except` fallbacks for test seeds; no finding IDs in code, tests, or commits.
- **Knowledge**: [embeddings-and-vectors](../../../knowledge/domains/agent/embeddings-and-vectors.md).

## Steps

### 1. Write failing tests (RED)

1. `src/py/tests/unit/db/test_spanner.py`:
   - Change `from vertebra.embeddings import SpannerEmbedder` to `from vertebra.embeddings import EmbeddingError, SpannerEmbedder`.
   - In `test_async_spanner_driver_and_embedder_and_datafusion`, delete the two lines `batch_view = await embedder.embed_many(["hello spanner", "vector pushdown"])` and `assert len(batch_view) == 2`.
   - Append:

   ```python
   class _StubSpannerSelectDriver:
       """Spanner driver stand-in that returns fixed rows or raises from select."""

       def __init__(self, rows: list[dict[str, Any]] | None = None, error: Exception | None = None) -> None:
           self.rows = list(rows or [])
           self.error = error
           self.calls: list[tuple[str, Any]] = []

       def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
           self.calls.append((sql, parameters))
           if self.error is not None:
               raise self.error
           return list(self.rows)


   @pytest.mark.anyio
   async def test_spanner_embedder_propagates_driver_errors() -> None:
       """Driver failures surface to the caller instead of becoming synthetic vectors."""
       driver = _StubSpannerSelectDriver(error=RuntimeError("model endpoint unavailable"))
       embedder = SpannerEmbedder(driver, dimensions=2, normalize=False)
       with pytest.raises(RuntimeError, match="model endpoint unavailable"):
           await embedder.embed_many(["alpha", "beta"])


   @pytest.mark.anyio
   @pytest.mark.parametrize(
       "rows",
       [[{"embedding": [0.6, 0.8]}], [{"embedding": [0.6, 0.8]}, {"other": 1}]],
       ids=["missing-row", "row-without-vector"],
   )
   async def test_spanner_embedder_rejects_row_count_mismatch(rows: list[dict[str, Any]]) -> None:
       """Missing rows or rows without a vector raise EmbeddingError."""
       embedder = SpannerEmbedder(_StubSpannerSelectDriver(rows=rows), dimensions=2, normalize=False)
       with pytest.raises(EmbeddingError):
           await embedder.embed_many(["alpha", "beta"])


   @pytest.mark.anyio
   async def test_spanner_embedder_returns_driver_vectors() -> None:
       """Rows map to texts in order, including the nested ``values`` shape."""
       driver = _StubSpannerSelectDriver(rows=[{"embedding": [0.6, 0.8]}, {"embedding": {"values": [0.0, 1.0]}}])
       embedder = SpannerEmbedder(driver, dimensions=2, normalize=False)
       batch = await embedder.embed_many(["alpha", "beta"])
       assert batch.to_list() == [pytest.approx([0.6, 0.8]), pytest.approx([0.0, 1.0])]
       assert driver.calls[0][1] == {"texts": ["alpha", "beta"]}


   def test_spanner_embedder_builders_reject_invalid_identifiers() -> None:
       """Spanner SQL builders reject model, table, column, and distance values that are not identifiers."""
       driver = _StubSpannerSelectDriver()
       with pytest.raises(ValueError, match="model_endpoint"):
           SpannerEmbedder(driver, model_endpoint="m, (SELECT 1)) --").build_generate_embedding_sql()
       embedder = SpannerEmbedder(driver)
       with pytest.raises(ValueError, match="base_table"):
           embedder.build_vector_search_sql("docs; DROP TABLE docs")
       with pytest.raises(ValueError, match="embedding_column"):
           embedder.build_vector_search_sql("docs", embedding_column="embedding) --")
       with pytest.raises(ValueError, match="distance_type"):
           embedder.build_vector_search_sql("docs", distance_type="MANHATTAN")


   @pytest.mark.parametrize("use_ai_function", [False, True], ids=["ml-predict", "ai-generate"])
   def test_spanner_embedder_generate_sql_orders_rows_by_input_index(use_ai_function: bool) -> None:
       """The generated SQL carries the input offset through the model and orders the output by it."""
       sql = SpannerEmbedder(_StubSpannerSelectDriver(), use_ai_function=use_ai_function).build_generate_embedding_sql()
       assert sql.startswith("SELECT embeddings.values AS embedding, idx FROM ")
       assert "(SELECT content, idx FROM UNNEST(@texts) AS content WITH OFFSET AS idx)" in sql
       assert sql.endswith(") ORDER BY idx")
   ```

2. `src/py/tests/unit/db/test_bigquery.py` (imports `BigQueryEmbedder`, `pytest`):
   - Change `from vertebra.embeddings import BigQueryEmbedder` to `from vertebra.embeddings import BigQueryEmbedder, EmbeddingError` (`Any` is already imported).
   - Append:

   ```python
   class _StubBigQuerySelectDriver:
       """BigQuery driver stand-in returning fixed rows from select."""

       def __init__(self, rows: list[dict[str, Any]]) -> None:
           self.rows = list(rows)

       def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
           return list(self.rows)


   def test_bigquery_embedder_builders_reject_invalid_identifiers() -> None:
       """BigQuery SQL builders validate model, table, column, and distance values."""
       with pytest.raises(ValueError, match="model_endpoint"):
           BigQueryEmbedder(object(), model="analytics.model`; DROP TABLE x; --").build_generate_embedding_sql()
       embedder = BigQueryEmbedder(object(), model="analytics-proj.analytics.text_embedding_model")
       assert "MODEL `analytics-proj.analytics.text_embedding_model`" in embedder.build_generate_embedding_sql()
       with pytest.raises(ValueError, match="base_table"):
           embedder.build_vector_search_sql("analytics.docs) --")
       with pytest.raises(ValueError, match="embedding_column"):
           embedder.build_vector_search_sql("analytics.docs", embedding_column="emb' OR '1")
       with pytest.raises(ValueError, match="distance_type"):
           embedder.build_vector_search_sql("analytics.docs", distance_type="MANHATTAN")


   @pytest.mark.parametrize("use_ai_function", [False, True], ids=["ml-generate", "ai-generate"])
   def test_bigquery_embedder_generate_sql_orders_rows_by_input_index(use_ai_function: bool) -> None:
       """The generated SQL carries the input offset through the model and orders the output by it."""
       sql = BigQueryEmbedder(
           object(), model="analytics.text_embedding_model", use_ai_function=use_ai_function
       ).build_generate_embedding_sql()
       assert sql.startswith("SELECT ") and ", idx FROM " in sql
       assert "(SELECT content, idx FROM UNNEST(@texts) AS content WITH OFFSET AS idx)" in sql
       assert sql.endswith(") ORDER BY idx")


   @pytest.mark.anyio
   @pytest.mark.parametrize(
       "rows",
       [[{"embedding": [0.25, 0.5]}], [{"embedding": [0.25, 0.5]}, {"embedding": []}]],
       ids=["missing-row", "empty-vector"],
   )
   async def test_bigquery_embedder_rejects_incomplete_results(rows: list[dict[str, Any]]) -> None:
       """A short result or an empty vector raises EmbeddingError."""
       embedder = BigQueryEmbedder(
           _StubBigQuerySelectDriver(rows), model="analytics.text_embedding_model", dimensions=2, normalize=False
       )
       with pytest.raises(EmbeddingError):
           await embedder.embed_many(["alpha", "beta"])
   ```

3. `src/py/tests/unit/embeddings/test_google_embedders.py`:
   - Add `EmbeddingError` to the existing `from vertebra.embeddings import (...)` block.
   - Append:

   ```python
   @pytest.mark.anyio
   async def test_oracle_embedder_raises_embedding_error_on_empty_result() -> None:
       """An empty DBMS_VECTOR result raises EmbeddingError instead of IndexError."""

       class EmptyOracleDriver:
           async def execute(self, sql: str, params: Any = None) -> None:
               return None

           async def fetch_all(self, sql: str, params: Any = None) -> list[tuple[bytes]]:
               return []

       token_cache: Any = _FakeTokenCache(prefix="mock-ora-vertex-token", quota_project=None)
       embedder = OracleEmbedder(
           driver=EmptyOracleDriver(),
           model="gemini-embedding-001",
           dimensions=4,
           provider="vertexai",
           project_number="123456789012",
           token_cache=token_cache,
       )
       with pytest.raises(EmbeddingError, match="no embedding row"):
           await embedder.embed_query("empty result")
   ```

4. Run RED:

   ```bash
   uv run pytest src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/embeddings/test_google_embedders.py -q
   ```

   Expected: all three modules fail at collection with `ImportError: cannot import name 'EmbeddingError'`. Without that import, the BigQuery builder test fails with `DID NOT RAISE`, both ordering tests fail on the `startswith` assertion, and the incomplete-result cases fail because no `EmbeddingError` is raised (the missing-row case returns one vector; the empty-vector case passes a ragged row to `VectorBatchView.from_f32_rows`, which either raises its own error or returns a malformed batch).

### 2. Implement (GREEN)

1. `src/py/vertebra/embeddings/_base.py`:
   - Add `from vertebra.typing import VertebraError` to the first-party imports.
   - Add `"EmbeddingError"` to `__all__` and define it after `EmbeddingTask`:

   ```python
   class EmbeddingError(VertebraError, RuntimeError):
       """Raised when an embedding provider returns no usable vector for a requested text."""
   ```

2. `src/py/vertebra/embeddings/__init__.py`: import `EmbeddingError` from `vertebra.embeddings._base` and add `"EmbeddingError"` to `__all__` (keep alphabetical order: after `"Embedder"`, before `"EmbeddingBroker"`).
3. `src/py/vertebra/embeddings/_spanner.py`:
   - Imports: remove `hashlib` and `math`; import `EmbeddingError` with the other `_base` names; add `from vertebra.utils.text import validate_identifier`.
   - Add a module constant after `__all__`:

   ```python
   _DISTANCE_FUNCTIONS: dict[bool, dict[str, str]] = {
       True: {
           "COSINE": "APPROX_COSINE_DISTANCE",
           "EUCLIDEAN": "APPROX_EUCLIDEAN_DISTANCE",
           "DOT_PRODUCT": "APPROX_DOT_PRODUCT",
       },
       False: {"COSINE": "COSINE_DISTANCE", "EUCLIDEAN": "EUCLIDEAN_DISTANCE", "DOT_PRODUCT": "DOT_PRODUCT"},
   }
   ```

   - `build_generate_embedding_sql`: validate the endpoint and emit the ordered query:

   ```python
   model_endpoint = validate_identifier(self._model_endpoint, "model_endpoint")
   fn_name = "AI.GENERATE_EMBEDDING" if self._use_ai_function else "ML.PREDICT"
   return (
       "SELECT embeddings.values AS embedding, idx "
       f"FROM {fn_name}(MODEL {model_endpoint}, "
       "(SELECT content, idx FROM UNNEST(@texts) AS content WITH OFFSET AS idx)) "
       "ORDER BY idx"
   )
   ```
   - `build_vector_search_sql`: first lines

   ```python
   validate_identifier(base_table, "base_table")
   validate_identifier(embedding_column, "embedding_column", allow_dots=False)
   distance_fn = _DISTANCE_FUNCTIONS[approximate].get(distance_type.upper())
   if distance_fn is None:
       msg = f"Unsupported distance_type {distance_type!r}; expected COSINE, EUCLIDEAN, or DOT_PRODUCT"
       raise ValueError(msg)
   ```

   then build `distance_expr` from `distance_fn` (keep the `num_leaves_to_search` options clause only for `approximate=True`) and drop the two inline `fn_map` dicts.
   - Replace `_embed_batch_impl` body (after `del task, titles`) with:

   ```python
   sql = self.build_generate_embedding_sql()
   result = self._driver.select(sql, {"texts": list(texts)})
   rows = list(await result) if asyncio.iscoroutine(result) else list(result)
   if len(rows) != len(texts):
       msg = f"Spanner returned {len(rows)} embedding rows for {len(texts)} texts"
       raise EmbeddingError(msg)
   vectors = [self._extract_vector(row, index=idx, dimensions=self._dimensions) for idx, row in enumerate(rows)]
   return VectorBatchView.from_f32_rows(vectors, normalize=self._normalize)
   ```

   Update its docstring with `Raises: EmbeddingError: When the row count differs from the text count or a row has no vector.`
   - Replace `_extract_vector` with:

   ```python
   @staticmethod
   def _extract_vector(row: Mapping[str, Any], *, index: int, dimensions: int) -> list[float]:
       """Extract an embedding vector from a Spanner result row, zero-padded to ``dimensions``.

       Raises:
           EmbeddingError: When the row has no non-empty vector column.
       """
       for key in ("embedding", "ml_generate_embedding_result", "values"):
           candidate = row.get(key)
           if isinstance(candidate, Mapping) and "values" in candidate:
               candidate = candidate["values"]
           if isinstance(candidate, (list, tuple)) and candidate:
               floats = [float(v) for v in candidate[:dimensions]]
               floats.extend([0.0] * (dimensions - len(floats)))
               return floats
       msg = f"Spanner embedding row {index} has no vector column ('embedding', 'ml_generate_embedding_result', or 'values')"
       raise EmbeddingError(msg)
   ```

   Add `Mapping` to the `TYPE_CHECKING` import from `collections.abc` only if it is used solely in annotations; it is also used in `isinstance`, so import `from collections.abc import Mapping, Sequence` at module top level and remove `Sequence` from the `TYPE_CHECKING` block.
   - Delete `_deterministic_vector`.
4. `src/py/vertebra/embeddings/_google.py::OracleEmbedder._embed_batch_impl`: import `EmbeddingError` from `vertebra.embeddings._base`; replace the loop header and cell extraction with

   ```python
   for idx, item_text in enumerate(texts):
       rows = await self._driver.fetch_all(sql, {"text": item_text, "params": params_json})
       if not rows:
           msg = f"Oracle DBMS_VECTOR.UTL_TO_EMBEDDING returned no embedding row for text index {idx}"
           raise EmbeddingError(msg)
       cell = rows[0][0] if isinstance(rows[0], (tuple, list)) else rows[0]
   ```

   Add `Raises: EmbeddingError: When Oracle returns no row for a text.` to the docstring.
5. `src/py/vertebra/embeddings/_bigquery.py`:
   - Add `from vertebra.utils.text import validate_identifier` and a module constant `_DISTANCE_TYPES: frozenset[str] = frozenset({"COSINE", "EUCLIDEAN", "DOT_PRODUCT"})`.
   - `build_generate_embedding_sql`: validate the endpoint and emit the ordered query:

   ```python
   model_endpoint = validate_identifier(self._model_endpoint, "model_endpoint", allow_hyphens=True)
   fn_name = "AI.GENERATE_EMBEDDING" if self._use_ai_function else "ML.GENERATE_EMBEDDING"
   result_col = "embedding" if self._use_ai_function else "ml_generate_embedding_result"
   return (
       f"SELECT {result_col} AS embedding, idx "
       f"FROM {fn_name}("
       f"MODEL `{model_endpoint}`, "
       "(SELECT content, idx FROM UNNEST(@texts) AS content WITH OFFSET AS idx), "
       "STRUCT(TRUE AS flatten_json_output, @task_type AS task_type, @output_dimensionality AS output_dimensionality)"
       ") ORDER BY idx"
   )
   ```

   The existing `_MockBigQueryRestServer` in `test_bigquery.py` matches on `"ML.GENERATE_EMBEDDING"` in the SQL text, so `test_bigquery_embedder_and_datafusion_registration` keeps passing.
   - `_embed_batch_impl`: after `rows = await self._execute_driver_rows(sql, params)`, replace the vector loop with

   ```python
   if len(rows) != len(texts):
       msg = f"BigQuery returned {len(rows)} embedding rows for {len(texts)} texts"
       raise EmbeddingError(msg)
   vectors: list[list[float]] = []
   for idx, row in enumerate(rows):
       raw_vec = _extract_embedding_from_row(row)
       if not raw_vec:
           msg = f"BigQuery embedding row {idx} has no vector"
           raise EmbeddingError(msg)
       vectors.append(raw_vec[: self._dimensions])
   ```

   and import `EmbeddingError` from `vertebra.embeddings._base`. Add `Raises: EmbeddingError` to its docstring (add a one-line docstring if it has none: "Generate embeddings via BigQuery SQL and pack them into a ``VectorBatchView``.").
   - `build_vector_search_sql`: first lines

   ```python
   validate_identifier(base_table, "base_table", allow_hyphens=True)
   validate_identifier(embedding_column, "embedding_column", allow_dots=False)
   dist = distance_type.upper()
   if dist not in _DISTANCE_TYPES:
       msg = f"Unsupported distance_type {distance_type!r}; expected COSINE, EUCLIDEAN, or DOT_PRODUCT"
       raise ValueError(msg)
   ```

   Add `Raises: ValueError` lines to both builder docstrings.
6. Docs `docs/usage/embeddings-and-vectors.md`, section `## Google Cloud & In-Database SQL Pushdown`: append the paragraph `` In-database embedders raise `EmbeddingError` (a `RuntimeError`) when the database returns no vector for a text, and propagate driver errors unchanged. Model, table, and column names passed to the SQL builders must be plain dotted identifiers. ``
7. Knowledge `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md`, in the "Google-First Zero-Weight Cloud & SQL Pushdown" bullet, append: `` SQL pushdown embedders (`SpannerEmbedder`, `OracleEmbedder`, `BigQueryEmbedder`) raise `EmbeddingError(VertebraError, RuntimeError)` for missing vectors or row-count mismatches, propagate driver errors, order generated embeddings by input offset (`ORDER BY idx`), and validate interpolated identifiers with `validate_identifier`. ``
8. Changelog `docs/changelog/index.md`, `## 0.3.0`:
   - `### Added`: `` - `EmbeddingError` in `vertebra.embeddings` for providers that return no usable vector. ``
   - `### Fixed`: `` - `SpannerEmbedder` propagates driver errors and raises `EmbeddingError` on missing rows instead of returning hash vectors; `BigQueryEmbedder` raises `EmbeddingError` on short or vector-less results; both order generated embeddings by input offset; `OracleEmbedder` raises `EmbeddingError` on empty results; `BigQueryEmbedder` and `SpannerEmbedder` SQL builders validate model, table, column, and distance values. ``

### 3. Refactor & verify

1. Run the GREEN commands; run `make lint`.
2. Commit:

   ```bash
   git add src/py/vertebra/embeddings/_base.py src/py/vertebra/embeddings/__init__.py src/py/vertebra/embeddings/_spanner.py src/py/vertebra/embeddings/_google.py src/py/vertebra/embeddings/_bigquery.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/embeddings/test_google_embedders.py docs/usage/embeddings-and-vectors.md .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md docs/changelog/index.md
   git commit -S -m "fix(embeddings): propagate remote embedder errors and validate SQL identifiers"
   ```

## Verification

- **RED**: the Step 1.4 command exits `1`/`2` with the collection errors and failures described.
- **GREEN**:

  ```bash
  uv run pytest src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/embeddings -q
  uv run ruff check src/py/vertebra/embeddings src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/embeddings/test_google_embedders.py
  uv run ruff format --check src/py/vertebra/embeddings src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/embeddings/test_google_embedders.py
  uv run mypy src/py/vertebra/embeddings
  ```

  All exit `0`.
- **Phase gate**: Phase 2 checkpoint in `spec.md`.

## Acceptance Criteria

- [x] R10: `SpannerEmbedder` propagates driver errors, raises `EmbeddingError` on count mismatch and vector-less rows, and has no `_deterministic_vector`.
- [x] R10: `OracleEmbedder` raises `EmbeddingError` on an empty result.
- [x] R10: `BigQueryEmbedder` raises `EmbeddingError` on a row-count mismatch or an empty vector.
- [x] R10: Both generate builders select the vector first, pass `idx` through the model function as the second column, and end with `ORDER BY idx`.
- [x] R10: BigQuery and Spanner builders raise `ValueError` for invalid model, table, column, and distance values; valid hyphenated BigQuery names still build.
- [x] `EmbeddingError` exported from `vertebra.embeddings`; docs, knowledge, and changelog updated; targeted checks pass; one signed commit.

## Notes & Discoveries

- [2026-11-03 15:30] Planning: the existing Spanner embedder assertion only passed because SQLite errors were replaced with hash vectors.
- [2026-11-03 17:01] Verified RED (`3 errors` at collection, `12 failed, 18 passed` after adding `EmbeddingError` stub) and GREEN (`41 passed in 3.18s`, `ruff check`, `ruff format --check`, and `mypy` passed on 9 source files).
