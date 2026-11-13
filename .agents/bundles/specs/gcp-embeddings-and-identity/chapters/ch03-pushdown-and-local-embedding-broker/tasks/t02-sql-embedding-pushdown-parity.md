---
type: Task
id: "gcp-embeddings-and-identity:ch03:t02"
flow_id: gcp-embeddings-and-identity
chapter_id: ch03-pushdown-and-local-embedding-broker
title: "Align BigQuery, Spanner, and PostgreSQL SQL Embedding Pushdown Contracts"
description: "Add embed_batch, top_k > 0 validation, boolean rejection, and exact dimensions enforcement across BigQuery, Spanner, and PostgreSQL pushdowns, and bind task_type and output_dimensionality on Spanner AI.GENERATE_EMBEDDING."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch03:t01"
files:
  - src/py/vertebra/db/adapters/bigquery/embeddings.py
  - src/py/vertebra/db/adapters/spanner/embeddings.py
  - src/py/vertebra/db/adapters/postgres/embeddings.py
tests:
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
read_set:
  - src/py/vertebra/db/adapters/bigquery/embeddings.py
  - src/py/vertebra/db/adapters/spanner/embeddings.py
  - src/py/vertebra/db/adapters/postgres/embeddings.py
  - src/py/tests/unit/db/test_bigquery.py
  - src/py/tests/unit/db/test_spanner.py
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
estimated_loc: 165
tags:
  - bigquery
  - spanner
  - postgres
  - embeddings
  - pushdown
sources:
  - id: py-bq-pushdown
    resource: src/py/vertebra/db/adapters/bigquery/embeddings.py::BigQueryEmbeddingPushdown
    title: "BigQuery SQL embedding pushdown"
  - id: py-spanner-pushdown
    resource: src/py/vertebra/db/adapters/spanner/embeddings.py::SpannerEmbeddingPushdown
    title: "Spanner SQL embedding pushdown"
  - id: py-pg-pushdown
    resource: src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown
    title: "PostgreSQL SQL embedding pushdown"
---

# Task `t02`: Align BigQuery, Spanner, and PostgreSQL SQL Embedding Pushdown Contracts

## Objective & Scope Boundary

Bring `BigQueryEmbeddingPushdown`, `SpannerEmbeddingPushdown`, and `PostgresEmbeddingPushdown` to full API and validation parity with `OracleEmbeddingPushdown` per [`../spec.md` §2](../spec.md) [^py-bq-pushdown] [^py-spanner-pushdown] [^py-pg-pushdown]. Out of scope: local `EmbeddingBroker` and `GemmaEmbedder` (`t03`–`t04`).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/bigquery/embeddings.py` — `BigQueryEmbeddingPushdown` [^py-bq-pushdown].
- `src/py/vertebra/db/adapters/spanner/embeddings.py` — `SpannerEmbeddingPushdown` [^py-spanner-pushdown].
- `src/py/vertebra/db/adapters/postgres/embeddings.py` — `PostgresEmbeddingPushdown` [^py-pg-pushdown].
- `src/py/tests/unit/db/test_bigquery.py`, `test_spanner.py`, and `test_pg_dataplane_and_datafusion.py` — adapter pushdown tests.

## Contracts & Edge Cases

- Add `async def embed_batch(self, texts: Sequence[str], *, task: str = "document", dimensions: int | None = None, normalize: bool = True) -> VectorBatchView` delegating to `embed_documents` on all three classes.
- Validate `top_k > 0` (`raise ValueError("top_k must be positive")`) in `similarity_search` across all three classes.
- In `_extract_vector` on all three classes, reject `isinstance(value, bool)` (`True`/`False` must never coerce to `1.0`/`0.0`) and require `len(candidate) == dimensions` (`DataError` when fewer or more elements are returned).
- In `SpannerEmbeddingPushdown`, when `self._use_ai_function` is `True`, emit `STRUCT(TRUE AS flatten_json_output, @task_type AS task_type, @output_dimensionality AS output_dimensionality)` in `build_generate_embedding_sql()` and bind `task_type` (via `_TASK_TO_VERTEX_TASK_TYPE`) and `output_dimensionality = dims` in `embed_documents` (leaving `ML.PREDICT` SQL and `@texts`-only binds unchanged when `use_ai_function=False`).

## Test Matrix

- Parametrized tests across `test_bigquery.py`, `test_spanner.py`, and `test_pg_dataplane_and_datafusion.py`: `embed_batch` output parity with `embed_documents`; `SpannerEmbeddingPushdown(..., use_ai_function=True)` SQL + `@task_type` (`RETRIEVAL_QUERY` vs `RETRIEVAL_DOCUMENT`) + `@output_dimensionality` binds vs `use_ai_function=False` `ML.PREDICT` binds; `DataError` on boolean `[True, False, ...]` or oversized/undersized vector cells; `ValueError` on `top_k=0` or `top_k=-1`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/adapters/{bigquery,spanner,postgres}/embeddings.py && uv run --no-sync mypy src/py/vertebra/db/adapters/{bigquery,spanner,postgres}/embeddings.py && uv run --no-sync pytest src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -k "embed or vector" -q
```

## Acceptance Criteria

- [ ] BigQuery, Spanner, and PostgreSQL pushdowns expose `embed_batch`, enforce `top_k > 0`, boolean-free vectors, and exact `dimensions`, and Spanner `AI.GENERATE_EMBEDDING` binds `task_type` and `output_dimensionality` (`R12`).
- [ ] Zero regressions across BigQuery, Spanner, and PostgreSQL unit suites.

[^py-bq-pushdown]: `src/py/vertebra/db/adapters/bigquery/embeddings.py::BigQueryEmbeddingPushdown` — BigQuery SQL embedding pushdown.
[^py-spanner-pushdown]: `src/py/vertebra/db/adapters/spanner/embeddings.py::SpannerEmbeddingPushdown` — Spanner SQL embedding pushdown.
[^py-pg-pushdown]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — PostgreSQL SQL embedding pushdown.
