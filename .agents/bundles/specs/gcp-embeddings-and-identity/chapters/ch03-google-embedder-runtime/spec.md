---
type: Spec
parent_flow: gcp-embeddings-and-identity
chapter_id: ch03-google-embedder-runtime
title: "Chapter 03: Google Embedder Runtime"
description: Shared _GoogleEmbedder authentication and error base, per-event-loop micro-batch queues, and explicit-cache Oracle credential rotation.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-id-token-core
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:35:00Z"
sources:
  - id: py-google-embedders
    resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder
    title: Token cache resolution, 401 retry, micro-batcher, and GeminiEmbedder API-key override at HEAD
  - id: py-embedder-base
    resource: src/py/vertebra/embeddings/_base.py::Embedder
    title: Embedder facade, EmbeddingError, and sync portal loop
  - id: py-oracle-pushdown
    resource: src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown._ensure_oracle_credential_fresh
    title: Oracle credential rotation with storage fallback and silent ValueError skip
  - id: py-embedder-tests
    resource: src/py/tests/unit/embeddings/test_google_embedders.py
    title: Canonical embedder tests with _FakeTokenCache and injected transports
tags:
  - embeddings
  - gcp
  - asyncio
  - fail-loud
---

# Chapter 03: Google Embedder Runtime

<!-- truth: start -->
- **Scope**: `ch03:t01` introduces `_GoogleEmbedder` in `src/py/vertebra/embeddings/_google.py` as the base of `VertexEmbedder` and `GeminiEmbedder` (token-cache normalization, default ADC cache without storage fallback, single `401` retry, `EmbeddingError` mapping, row-count validation) [^py-google-embedders]; `ch03:t02` replaces the shared `_pending` / `_flush_task` pair with per-event-loop queues [^py-embedder-base]; `ch03:t03` makes `OracleEmbeddingPushdown` rotate credentials only from an explicit token cache and propagate token errors [^py-oracle-pushdown].
- **Invariants**: no `vertebra.storage` import from embedders; synchronous `SyncGcpTokenCache` instances are converted with `.to_async()` so refreshes never block the event loop; every non-`200` response or gRPC error status raises `EmbeddingError`; futures resolve only on the loop that created them.
- **Verification Gate**: `uv run pytest src/py/tests/unit/embeddings/test_google_embedders.py -q && uv run mypy src/py/vertebra/embeddings src/py/vertebra/db/adapters/oracle/embeddings.py`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

At `c8722c76`, `VertexEmbedder._ensure_token_cache` forces `{"use_metadata_server": "true"}` (bypassing the ambient ADC file on developer machines), catches `TypeError` to construct a cache from `vertebra.storage._store`, and treats `SyncGcpTokenCache.get_token()` results with `inspect.isawaitable`, so an injected sync cache refreshes on the event loop thread. `_dispatch_with_auth_retry` raises bare `RuntimeError`. One `_pending` list and one `_flush_task` serve every event loop, so a call from the `Embedder._run_sync` portal loop and a call from the main loop can land in one drain task whose futures belong to different loops. No path checks that the provider returned one row per input [^py-google-embedders].

`OracleEmbeddingPushdown._ensure_oracle_credential_fresh` repeats the forced metadata cache and storage fallback, and returns silently on `ValueError`, so credential rotation can stop without notice [^py-oracle-pushdown].

### 1.2 Non-Goals

- Vertex endpoint and payload changes (`ch05`) and Gemini wire changes (`ch04`); this chapter keeps both request formats and moves only the shared runtime.
- Changing `Embedder` public method signatures.

### 1.3 Upstream Baseline & Code Analysis

- `ch01:t03` makes core ADC discovery end at the metadata server, so a cache built from empty options already covers GCE, Cloud Run, and developer ADC files.
- `GeminiEmbedder` subclasses `VertexEmbedder` at HEAD and overrides `_dispatch_with_auth_retry` for API keys. After `t01`, both subclass `_GoogleEmbedder` and `GeminiEmbedder` overrides only `_auth_headers`.
- Tests in `test_google_embedders.py` [^py-embedder-tests] use `_FakeTokenCache` (async `get_token(*, force_refresh=False)`, `invalidate(generation)`) and injected `transport` callables.

Applicable guidance: [python-framework SKILL.md](../../../../../skills/python-framework/SKILL.md), [embeddings-and-vectors.md](../../../../knowledge/domains/agent/embeddings-and-vectors.md).

## 2. Requirements, Concrete Contracts & Architecture

### Concrete Module & Signature Contracts

```python
class _GoogleEmbedder(Embedder):
    DEFAULT_SCOPES: ClassVar[tuple[str, ...]] = ("https://www.googleapis.com/auth/cloud-platform",)
    PROVIDER: ClassVar[str] = "Google"
    def __init__(self, model: str, *, dimensions: int, endpoint: str,
                 token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None,
                 target_service_account: str | None, quota_project_id: str | None,
                 cache: VectorCache | None, max_batch_size: int, max_batch_tokens: int,
                 coalesce_window_ms: float, transport: GrpcTransportCallable | None) -> None: ...
    @property
    def endpoint(self) -> str: ...
    @property
    def resource_model_name(self) -> str: ...
    async def _post_authorized(self, url: str, headers: dict[str, str], body: bytes) -> tuple[dict[str, str], bytes]: ...
    async def _auth_headers(self, *, force_refresh: bool) -> tuple[dict[str, str], int | None]: ...
    def _resolve_token_cache(self) -> AsyncGcpTokenCache: ...
    @staticmethod
    def _require_rows(batch: VectorBatchView, expected: int, provider: str) -> VectorBatchView: ...

class _MicroBatchQueue(msgspec.Struct):
    pending: list[_PendingMicroBatchItem]
    flush_task: asyncio.Task[None] | None = None
```

### Caller -> Seam -> Collaborator Map

- `Embedder.embed_query` -> `_GoogleEmbedder._embed_single_impl` -> per-loop `_MicroBatchQueue` -> `_drain_micro_batches(loop_queue)` -> subclass `_embed_batch_impl` -> `_GoogleEmbedder._post_authorized` -> `_auth_headers` (token cache or API key) -> transport -> `EmbeddingError` on failure.
- `OracleEmbeddingPushdown.embed_query` / `embed_documents` -> `_ensure_oracle_credential_fresh` -> explicit token cache only -> `DBMS_VECTOR.DROP_CREDENTIAL` / `CREATE_CREDENTIAL`.

### Edge-Case & Invariant Matrix

| Scenario | Input | Expected | Owner |
| --- | --- | --- | --- |
| Sync cache injected | `VertexEmbedder(token_cache=SyncGcpTokenCache(...))` | Stored cache is the `.to_async()` handle | `t01` |
| No credentials | Default cache construction raises `ValueError` | `ImproperConfigurationError` naming the provider, raised on first request | `t01` |
| Second 401 | Transport returns `401` twice | `EmbeddingError` containing `HTTP 401`; exactly two transport calls | `t01` |
| gRPC error | HTTP `200`, `grpc-status: 3`, `grpc-message: bad%20model` | `EmbeddingError("Gemini embedding gRPC status 3: bad model")` | `t01` |
| Row drift | Decoder yields `1` row for `2` texts | `EmbeddingError` containing `returned 1 embeddings for 2 inputs` | `t01` |
| Cross-loop | Portal-loop `embed_query_sync` and main-loop `embed_query` concurrently | Two queues; both callers receive results | `t02` |
| Drain cancelled | Drain task cancelled mid-request | Pending futures receive `asyncio.CancelledError` | `t02` |
| Oracle no cache | `token_cache=None` | No credential SQL | `t03` |
| Oracle cache error | `get_token` raises `RuntimeError("denied")` | Propagates from `embed_query` | `t03` |

### Alternatives Comparison

- Keeping `GeminiEmbedder` as a `VertexEmbedder` subclass would force Gemini to inherit Vertex project/location semantics and REST payloads after `ch05`; a shared private base keeps each transport local to its class.
- A `weakref.WeakKeyDictionary` keyed by loop depends on loop implementations supporting weak references; a lock-guarded `dict` keyed by the loop object, with entries removed when a queue drains empty, works for asyncio and uvloop.

## 3. Documentation & OKF Knowledge Plan

- Runtime invariants are documented in `ch06:t01` (`docs/usage/embeddings-and-vectors.md`) and `ch06:t02` (`embeddings-and-vectors.md` OKF).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Canonical Test |
| --- | --- | --- | --- |
| R9 (auth base, errors, rows) | `t01` | `regression_tdd` | `test_google_embedders.py::test_google_embedder_*` |
| R9 (per-loop batching) | `t02` | `regression_tdd` | `test_google_embedders.py::test_micro_batch_*` |
| R10 | `t03` | `regression_tdd` | `test_google_embedders.py::test_oracle_*` |

## 5. Implementation Plan

- [ ] t01 — Shared Google embedder authentication and error base (tasks/t01-google-embedder-auth-base.md)
- [ ] t02 — Per-event-loop micro-batch queues (tasks/t02-per-loop-micro-batching.md)
- [ ] t03 — Explicit-cache Oracle credential rotation (tasks/t03-oracle-credential-rotation-fail-loud.md)
- **Chapter Recovery Checkpoint**: `uv run pytest src/py/tests/unit/embeddings -q && uv run ruff check src/py/vertebra/embeddings src/py/vertebra/db/adapters/oracle src/py/tests/unit/embeddings && uv run mypy src/py/vertebra/embeddings src/py/vertebra/db/adapters/oracle`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/3` tasks completed; depends on `ch01` (ADC order).

[^py-google-embedders]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder` — Runtime being refactored.
[^py-embedder-base]: `src/py/vertebra/embeddings/_base.py::Embedder` — Facade and portal loop.
[^py-oracle-pushdown]: `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown._ensure_oracle_credential_fresh` — Oracle rotation.
[^py-embedder-tests]: `src/py/tests/unit/embeddings/test_google_embedders.py` — Canonical tests.
