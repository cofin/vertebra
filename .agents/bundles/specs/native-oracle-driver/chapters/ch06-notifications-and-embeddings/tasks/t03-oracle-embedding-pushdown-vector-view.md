---
type: Task
id: "native-oracle-driver:ch06:t03"
flow_id: native-oracle-driver
chapter_id: ch06-notifications-and-embeddings
title: "Task 03: Align OracleEmbeddingPushdown with Native VectorView"
description: Preserve OracleEmbeddingPushdown public method signatures in src/py/vertebra/db/adapters/oracle/embeddings.py while extending _extract_vector and _extract_batch_vectors to unwrap native VectorView, ORACLE_VECTOR_MAGIC bytes, and dict/tuple row shapes returned by _oracle.
status: stable
state: completed
priority: P1
owner: null
depends_on:
  - "native-oracle-driver:ch06:t02"
files:
  - src/py/vertebra/db/adapters/oracle/embeddings.py
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch06-notifications-and-embeddings/spec.md
  - src/py/vertebra/db/adapters/oracle/embeddings.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/rs/vertebra-py/src/vector.rs
verification_strategy: behavior_tdd
estimated_loc: 120
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
claimed_by: agent:executor
claimed_at: "2026-11-09T17:33:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T17:36:00Z"
last_verified_commit: 2f0e7454
verification_evidence: "pytest src/py/tests/unit/db/test_oracle_adapter.py (84 passed) and ruff/mypy clean"
created_at: "2026-11-09T00:48:00Z"
updated_at: "2026-11-09T17:36:00Z"
completed_at: "2026-11-09T17:36:00Z"
commit_sha: null
commit: null
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public contracts for Oracle 26ai VECTOR codecs and OracleEmbeddingPushdown
  - id: ora-embeddings-py
    resource: src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown
    title: Oracle 26ai UTL_TO_EMBEDDINGS and VECTOR_DISTANCE pushdown helper
  - id: ora-core-py
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: OracleCore and decode_vector_f32 helpers
  - id: ora-rs-vector
    resource: src/rs/vertebra-py/src/vector.rs::VectorView
    title: Native Rust VectorView and VectorBatchView PyO3 classes in vertebra._vertebra
tags:
  - oracle
  - vector
  - embeddings
  - 26ai
---

# Task 03: Align OracleEmbeddingPushdown with Native VectorView

## Objective & Scope Boundary

Preserve all existing public `OracleEmbeddingPushdown` method signatures in `src/py/vertebra/db/adapters/oracle/embeddings.py` (`embed_documents -> VectorBatchView`, `embed_batch -> VectorBatchView`, `embed_query -> VectorView`, `similarity_search -> list[dict[str, Any]]`) while extending `_extract_vector` and `_extract_batch_vectors` to unwrap native `VectorView` instances, `ORACLE_VECTOR_MAGIC` `bytes`/`memoryview` (`decode_oracle_vector_f32` / `OracleCore.decode_vector_f32`), and dict/tuple row shapes returned by native `vertebra._oracle` execution [^ora-contracts] [^ora-embeddings-py] [^ora-core-py] [^ora-rs-vector].

- **In Scope**:
  - Preserve the exact public `EmbeddingPushdown` signatures on `OracleEmbeddingPushdown`.
  - Extend `OracleEmbeddingPushdown._extract_vector(row: Any, dimensions: int) -> list[float]` to unwrap `VectorView` (`cell.to_list()` when `isinstance(cell, VectorView)` or `row` itself is a `VectorView`), `ORACLE_VECTOR_MAGIC` `bytes | bytearray | memoryview` (`decode_oracle_vector_f32` / `decode_vector_f32`), JSON strings (`decode_json`), and nested `Mapping` (`{"embed_vector": ...}` / `{"EMBED_VECTOR": ...}`) or sequence payloads, raising `DataError("Oracle embedding row has no valid vector")` on invalid or truncated rows.
  - Extend `OracleEmbeddingPushdown._extract_batch_vectors(cls, rows: Sequence[Any], expected_count: int, dimensions: int) -> list[list[float]]` to accept single-row JSON/OSON batch arrays containing `VectorView` items, nested dicts (`embed_vector` / `embed_data` in lowercase or uppercase), raw binary `VECTOR` bytes, or float lists.
- **Out of Scope**:
  - Rust `VectorView` implementation in `src/rs/vertebra-py/src/vector.rs` (already implemented and tested).

## Bounded Read Manifest

1. `.agents/bundles/specs/native-oracle-driver/contracts.md` — Section 4 (`VECTOR` codec and `enable_lowercase_column_names` rules) and Section 5 (`OracleEmbeddingPushdown`).
2. `.agents/bundles/specs/native-oracle-driver/chapters/ch06-notifications-and-embeddings/spec.md` — Section 2.3 `VectorView` extraction specification.
3. `src/py/vertebra/db/adapters/oracle/embeddings.py` — Current `OracleEmbeddingPushdown` implementation.
4. `src/py/vertebra/db/adapters/oracle/core.py` — `OracleCore` and `decode_vector_f32` / `encode_vector_f32`.
5. `src/rs/vertebra-py/src/vector.rs` — `VectorView` and `VectorBatchView` PyO3 bindings.

## Symbol & Signature Contracts

### `src/py/vertebra/db/adapters/oracle/embeddings.py`

```python
class OracleEmbeddingPushdown:
    async def embed_documents(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorBatchView: ...

    async def embed_batch(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorBatchView: ...

    async def embed_query(
        self,
        text: str,
        *,
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorView: ...

    async def similarity_search(
        self,
        query_vector: VectorView | Sequence[float],
        *,
        table: str,
        embedding_column: str = "embedding",
        top_k: int = 10,
        metric: str = "cosine",
    ) -> list[dict[str, Any]]: ...

    @classmethod
    def _extract_batch_vectors(
        cls,
        rows: Sequence[Any],
        expected_count: int,
        dimensions: int,
    ) -> list[list[float]]: ...

    @staticmethod
    def _extract_vector(row: Any, dimensions: int) -> list[float]: ...
```

## Edge Cases & Error Semantics

- **`VectorView` Unwrapping in `_extract_vector` and `_extract_batch_vectors`**: When `row` or its extracted cell is a `VectorView` (or a dict containing `"embed_vector"` / `"EMBED_VECTOR"` / `"embed_data"` / `"EMBED_DATA"`), `_extract_vector` calls `cell.to_list()` before validating `len(cell) >= dimensions` and slicing `[:dimensions]`.
- **Case-Insensitive Column Lookup in `_extract_batch_vectors`**: Because `enable_lowercase_column_names=True` normalizes unquoted Oracle projection aliases to lowercase (`"embed_vector"`, `"embed_data"`) while quoted projections may use `"EMBED_VECTOR"` / `"EMBED_DATA"`, `_extract_batch_vectors` and `_extract_vector` inspect both lowercase and uppercase keys as well as single-column dict values.
- **Invalid or Short Vector Error Contract**: Any missing, non-numeric, or under-dimensioned (`len < dimensions`) vector payload raises `DataError("Oracle embedding row has no valid vector")`.

## Public-Seam Test Blueprint

Add unit tests in `src/py/tests/unit/db/test_oracle_adapter.py`:
- `test_oracle_embedding_pushdown_extracts_native_vector_view_bytes_and_sequence`: constructs a native `VectorView` (`VectorBatchView.from_f32_rows([[0.25, -0.5, 1.0]], normalize=False)[0]`), raw `ORACLE_VECTOR_MAGIC` `encode_vector_f32([0.25, -0.5, 1.0])` bytes, and a tuple row `([0.25, -0.5, 1.0],)`, asserting `_extract_vector(row, 3)` returns `[0.25, -0.5, 1.0]` for all three and raises `DataError("Oracle embedding row has no valid vector")` on invalid or short input.
- `test_oracle_embedding_pushdown_batch_vectors_handles_vector_view_and_lowercase_json`: verifies `_extract_batch_vectors` and `embed_documents` return a `VectorBatchView` whether the driver returns `[{"utl_to_embeddings": [VectorView(...), VectorView(...)]}]`, `[{"embed_data": {"embed_vector": [0.1, 0.2, 0.3]}} ...]`, or JSON strings.
- `test_oracle_embedding_pushdown_similarity_search_accepts_vector_view`: passes a `VectorView` as `query_vector` to `await pushdown.similarity_search(vec_view, table="docs", embedding_column="embedding", top_k=5, metric="cosine")` and verifies the bound parameter and SQL execution.

## Verification

```bash
uv run pytest src/py/tests/unit/db/test_oracle_adapter.py -k "embedding"
make lint-all
```

## Acceptance Criteria

- [x] `OracleEmbeddingPushdown` in `src/py/vertebra/db/adapters/oracle/embeddings.py` preserves its `embed_documents`, `embed_batch`, `embed_query`, `similarity_search`, `_extract_batch_vectors`, and `_extract_vector` signatures [^ora-embeddings-py].
- [x] `_extract_vector` and `_extract_batch_vectors` unwrap native `VectorView` instances, `ORACLE_VECTOR_MAGIC` `bytes`/`memoryview`, and lowercase/uppercase dict or tuple row shapes returned by `vertebra._oracle`, raising `DataError("Oracle embedding row has no valid vector")` on malformed rows [^ora-contracts] [^ora-rs-vector].
- [x] Unit tests in `src/py/tests/unit/db/test_oracle_adapter.py` and `make lint-all` pass with zero errors.

## Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public contracts for Oracle 26ai `VECTOR` codecs and `OracleEmbeddingPushdown`.
[^ora-embeddings-py]: `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown` — Oracle 26ai `UTL_TO_EMBEDDINGS` and `VECTOR_DISTANCE` pushdown helper.
[^ora-core-py]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — `OracleCore` and `decode_vector_f32` helpers.
[^ora-rs-vector]: `src/rs/vertebra-py/src/vector.rs::VectorView` — Native Rust `VectorView` and `VectorBatchView` PyO3 classes in `vertebra._vertebra`.
