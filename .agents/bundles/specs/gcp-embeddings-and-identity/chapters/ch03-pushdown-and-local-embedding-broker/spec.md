---
type: Spec
flow_id: gcp-embeddings-and-identity
chapter_id: ch03-pushdown-and-local-embedding-broker
title: "Chapter 03: SQL Embedding Pushdown Parity and Local EmbeddingBroker Hardening"
description: "Makes Oracle credential rotation explicit and fail-loud, aligns BigQuery, Spanner, and PostgreSQL embedding pushdown contracts, isolates EmbeddingBroker state per event loop with verified SHM cleanup, and forwards GemmaEmbedder precision options."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch01-gcp-id-token-cache
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - embeddings
  - pushdown
  - broker
  - asyncio
  - gemma
sources:
  - id: py-oracle-pushdown
    resource: src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown
    title: "Oracle 26ai DBMS_VECTOR pushdown and _ensure_oracle_credential_fresh"
  - id: py-bq-pushdown
    resource: src/py/vertebra/db/adapters/bigquery/embeddings.py::BigQueryEmbeddingPushdown
    title: "BigQuery SQL embedding pushdown"
  - id: py-spanner-pushdown
    resource: src/py/vertebra/db/adapters/spanner/embeddings.py::SpannerEmbeddingPushdown
    title: "Spanner SQL embedding pushdown"
  - id: py-pg-pushdown
    resource: src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown
    title: "PostgreSQL / AlloyDB SQL embedding pushdown"
  - id: py-broker
    resource: src/py/vertebra/embeddings/_broker.py::EmbeddingBroker
    title: "Local EmbeddingBroker loop-bound queue/lock state and sidecar SHM IPC"
  - id: py-local
    resource: src/py/vertebra/embeddings/_local.py::GemmaEmbedder
    title: "GemmaEmbedder precision validation and FastEmbedder.build_backend factory"
---

# Chapter 03: SQL Embedding Pushdown Parity and Local EmbeddingBroker Hardening

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `OracleEmbeddingPushdown` auto-creates metadata token caches with `vertebra.storage._store` fallbacks and swallows `ValueError` [^py-oracle-pushdown]; `BigQueryEmbeddingPushdown`, `SpannerEmbeddingPushdown`, and `PostgresEmbeddingPushdown` omit `embed_batch`, accept boolean vector elements, silently slice oversized vectors, and omit `@task_type` / `@output_dimensionality` on Spanner `AI.GENERATE_EMBEDDING` [^py-bq-pushdown] [^py-spanner-pushdown] [^py-pg-pushdown]; `EmbeddingBroker` shares loop-bound `asyncio` primitives across event loops [^py-broker]; and `GemmaEmbedder` drops `self._precision` [^py-local].
- **Target Architecture**: All four SQL pushdown adapters expose uniform `embed_batch`, `top_k > 0`, boolean rejection, and exact `dimensions` validation, with explicit fail-loud Oracle credential rotation and Spanner `AI.GENERATE_EMBEDDING` task/dimension binding. `EmbeddingBroker` isolates `_pending`, `_flush_task`, and `_subprocess_lock` per running event loop and unlinks `/vtb-qvec-*` SHM segments cleanly, while `GemmaEmbedder` validates and forwards `precision` into `FastEmbedder.build_backend`.
- **Scope & Boundaries**: Covers `R10` and `R12` across `db/adapters/{oracle,bigquery,spanner,postgres}/embeddings.py` and `embeddings/{_broker,_local}.py` (`4` compact tasks).
<!-- truth: end -->

## 1. Specification, Context & Provenance

In-database SQL embedding pushdowns and local `EmbeddingBroker` / `GemmaEmbedder` runtimes share the same `VectorView` / `VectorBatchView` output contract as cloud embedders, yet exhibit drift across engines and multi-loop/free-threaded callers [^py-oracle-pushdown] [^py-broker] [^py-local]. On Python `3.12`–`3.15t`, calling `embed_query_sync` (via `PortalProvider`) alongside `await embed_query(...)` on a main event loop hits `EmbeddingBroker`'s single `_pending` queue, `_flush_task`, and `self._subprocess_lock = asyncio.Lock()`, raising cross-loop `RuntimeError`. Meanwhile, `GemmaEmbedder` stores `self._precision` (`int8`, `q4`, `bf16`, `fp32`) without passing it to `BackendFactory.options` [^py-local]. Guidance: [embeddings-and-vectors.md](../../../../knowledge/domains/agent/embeddings-and-vectors.md).

## 2. Requirements, Concrete Contracts & Architecture

```python
# Uniform SQL pushdown surface across OracleEmbeddingPushdown, BigQueryEmbeddingPushdown,
# SpannerEmbeddingPushdown, and PostgresEmbeddingPushdown:
async def embed_batch(
    self,
    texts: Sequence[str],
    *,
    task: str = "document",
    dimensions: int | None = None,
    normalize: bool = True,
) -> VectorBatchView: ...

# src/py/vertebra/embeddings/_broker.py
class _BrokerLoopState(msgspec.Struct, gc=False):
    pending: deque[_QueuedBrokerRequest]
    subprocess_lock: asyncio.Lock
    flush_task: asyncio.Task[None] | None = None
    subprocess: asyncio.subprocess.Process | None = None

# src/py/vertebra/embeddings/_local.py
class FastEmbedder(Embedder):
    @staticmethod
    def build_backend(
        model_name: str,
        *,
        cache_dir: str | None = None,
        specific_model_path: str | None = None,
        threads: int | None = None,
        precision: str | None = None,
    ) -> BackendInferenceCallable: ...
```

- **Oracle & SQL Pushdown Contracts**:
  - `OracleEmbeddingPushdown`: when `token_cache is None`, `_ensure_oracle_credential_fresh` is a no-op (leaving DBA-managed credentials untouched); when `token_cache` is supplied, `SyncGcpTokenCache` is normalized via `.to_async()` at init, zero `vertebra.storage._store` imports remain, and `get_token()` errors propagate unmodified; `top_k <= 0` raises `ValueError`; `_extract_vector` requires `len(cell) == dimensions` (`DataError` on length mismatch).
  - `BigQueryEmbeddingPushdown`, `SpannerEmbeddingPushdown`, `PostgresEmbeddingPushdown`: add `embed_batch` delegating to `embed_documents`; require `top_k > 0` (`ValueError` when `<= 0`); reject `isinstance(v, bool)` and `len(candidate) != dimensions` with `DataError`; on `SpannerEmbeddingPushdown` when `use_ai_function=True`, bind `STRUCT(TRUE AS flatten_json_output, @task_type AS task_type, @output_dimensionality AS output_dimensionality)` using Vertex task-type mapping.
- **Local `EmbeddingBroker` & `GemmaEmbedder` Contracts**:
  - `EmbeddingBroker`: replace instance-wide `_pending`, `_flush_task`, `_subprocess`, and `_subprocess_lock` with a `threading.Lock`-guarded `dict[asyncio.AbstractEventLoop, _BrokerLoopState]` so every running event loop owns its own `deque`, `Task`, `Lock`, and sidecar `Process`; `close()` terminates the active loop's sidecar (and any exited loop sidecars); `submit_batch_in_subprocess` unlinks any residual `/dev/shm/vtb-qvec-*` file in a `finally` block on sidecar crash, malformed JSON, or `resp.error`, and verifies `VectorBatchView.open_shm` unlinks the named SHM segment on open.
  - `GemmaEmbedder`: reject unsupported `precision` values outside `{"int8", "q4", "bf16", "fp32"}` with `ValueError` (keeping the `fp16`/`float16` activation-overflow message for Gemma 2), include `"precision": self._precision` in `BackendFactory.options`, and map `precision` inside `FastEmbedder.build_backend` (`quantize=True` for `"int8"` / `"q4"`, `quantize=False` for `"bf16"` / `"fp32"` when supported or explicit ONNX provider/model kwargs, forwarding cleanly without dropping the setting).

## 3. Documentation & OKF Knowledge Plan

Pushdown and local broker invariants are synchronized in `docs/usage/embeddings-and-vectors.md` and `embeddings-and-vectors.md` (`<= 200L`) in `ch04`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R10` (Explicit-cache fail-loud Oracle credential rotation & strict vector bounds) | `t01` | `src/py/vertebra/db/adapters/oracle/embeddings.py` | `pytest test_google_embedders.py test_oracle_adapter.py -k oracle` |
| `R12` (BigQuery, Spanner, Postgres `embed_batch`, task/dimension & bool parity) | `t02` | `src/py/vertebra/db/adapters/{bigquery,spanner,postgres}/embeddings.py` | `pytest test_bigquery.py test_spanner.py test_pg_dataplane_and_datafusion.py` |
| `R12` (`G-11`: per-event-loop `EmbeddingBroker` state & `/vtb-qvec-*` SHM cleanup) | `t03` | `src/py/vertebra/embeddings/_broker.py` | `pytest test_embedding_broker.py` |
| `R12` (`G-11`: `GemmaEmbedder` `precision` validation & `BackendFactory` forwarding) | `t04` | `src/py/vertebra/embeddings/_local.py` | `pytest test_embedding_broker.py -k gemma` |

## 5. Implementation Plan

- [ ] [t01-oracle-pushdown-explicit-credential-rotation.md](tasks/t01-oracle-pushdown-explicit-credential-rotation.md) — Remove implicit metadata/storage fallbacks from `OracleEmbeddingPushdown`, normalize sync token caches, propagate token errors, and enforce exact vector dimensions.
- [ ] [t02-sql-embedding-pushdown-parity.md](tasks/t02-sql-embedding-pushdown-parity.md) — Add `embed_batch`, `top_k > 0`, boolean-element rejection, and exact `dimensions` checks across BigQuery/Spanner/Postgres pushdowns plus Spanner `AI.GENERATE_EMBEDDING` task/dimension binds.
- [ ] [t03-embedding-broker-per-loop-and-shm-lifecycle.md](tasks/t03-embedding-broker-per-loop-and-shm-lifecycle.md) — Isolate `EmbeddingBroker` queue, lock, flush task, and sidecar per running event loop and guarantee `/dev/shm/vtb-qvec-*` unlink on success and error paths.
- [ ] [t04-gemma-embedder-precision-forwarding.md](tasks/t04-gemma-embedder-precision-forwarding.md) — Validate `GemmaEmbedder` `precision` modes and forward `precision` through `BackendFactory.options` into `FastEmbedder.build_backend`.

## 6. Current State & Remaining Plan Snapshot

- **Fast Chapter Gate (`< 20s`)**:
  `uv run --no-sync ruff check src/py/vertebra/embeddings src/py/vertebra/db/adapters/{oracle,bigquery,spanner,postgres}/embeddings.py && uv run --no-sync mypy src/py/vertebra/embeddings src/py/vertebra/db/adapters/{oracle,bigquery,spanner,postgres}/embeddings.py && uv run --no-sync pytest src/py/tests/unit/embeddings/test_embedding_broker.py src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/db/test_bigquery.py src/py/tests/unit/db/test_spanner.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -q`

[^py-oracle-pushdown]: `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown` — Oracle 26ai `DBMS_VECTOR` pushdown and `_ensure_oracle_credential_fresh`.
[^py-bq-pushdown]: `src/py/vertebra/db/adapters/bigquery/embeddings.py::BigQueryEmbeddingPushdown` — BigQuery SQL embedding pushdown.
[^py-spanner-pushdown]: `src/py/vertebra/db/adapters/spanner/embeddings.py::SpannerEmbeddingPushdown` — Spanner SQL embedding pushdown.
[^py-pg-pushdown]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — PostgreSQL / AlloyDB SQL embedding pushdown.
[^py-broker]: `src/py/vertebra/embeddings/_broker.py::EmbeddingBroker` — Local `EmbeddingBroker` loop-bound queue/lock state and sidecar SHM IPC.
[^py-local]: `src/py/vertebra/embeddings/_local.py::GemmaEmbedder` — `GemmaEmbedder` precision validation and `FastEmbedder.build_backend` factory.
