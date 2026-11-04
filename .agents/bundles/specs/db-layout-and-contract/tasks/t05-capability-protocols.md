---
type: Task
id: db-layout-and-contract:t05
title: Define runtime-checkable Sync/Async engine capability protocols and msgspec records
description: "Create `src/py/vertebra/db/driver/capabilities.py` defining the `@runtime_checkable` engine capability protocols (`SyncPipelineCapable`, `AsyncPipelineCapable`, `SyncPipeline`, `AsyncPipeline`, `NotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, `EmbeddingPushdown`) with complete typed signatures and `msgspec.Struct(..., frozen=True, gc=False)` value objects (`NotificationEvent`, `ClaimedJob`), re-exporting them from `vertebra.db.driver` and `vertebra.db`."
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - db-layout-and-contract:t04
files:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/driver/__init__.py
  - src/py/vertebra/db/__init__.py
tests:
  - src/py/tests/unit/db/test_native_db_drivers.py
plan_revision: 4
plan_commit: "f741701"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T21:40:00Z
updated_at: 2026-11-04T01:08:00Z
commit: null
tags: [db, capabilities, protocols, msgspec]
---

## Objective

Create `src/py/vertebra/db/driver/capabilities.py` defining the `@runtime_checkable` engine capability protocols (`SyncPipelineCapable`, `AsyncPipelineCapable`, `SyncPipeline`, `AsyncPipeline`, `NotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, `EmbeddingPushdown`) with complete typed signatures and `msgspec.Struct(..., frozen=True, gc=False)` value objects (`NotificationEvent`, `ClaimedJob`), re-exporting them from `vertebra.db.driver` and `vertebra.db`.

## Context

- **Verification strategy (`behavior_tdd`)**: `isinstance(..., <Protocol>)` structural checks across sync and async capability pairs, `msgspec.Struct` immutability and `gc=False` (`not gc.is_tracked(obj)`), and static Mypy structural subtyping are directly verifiable in unit tests.
- **Architectural invariants**:
  - Value objects inherit `FrozenBaseStruct` (`frozen=True, gc=False`) and use inline PEP 257 field docstrings (never `Attributes:` sections in the class docstring, enforced by `tools.docs.scan_public_docstrings`).
  - Dual-mode capabilities use explicit `Sync*` / `Async*` protocol pairs (`SyncPipelineCapable` returning `SyncPipeline` vs `AsyncPipelineCapable` returning `AsyncPipeline`, and `SyncChangeSource` returning `Iterator[ChangeEvent]` vs `AsyncChangeSource` returning `AsyncIterator[ChangeEvent]`) so `isinstance` narrows directly to the concrete sync or async protocol without union casts.
  - `db-engine-primitives`, `native-postgres-driver`, `native-oracle-driver`, `sqlspec-adapter-parity`, and `queues-tasks-and-channels` consume these exact protocol contracts without importing concrete adapter classes in generic layers.

## Steps

1. **Write failing protocol and `msgspec` record tests (RED)**:
   - In `src/py/tests/unit/db/test_native_db_drivers.py`, add parameterized tests asserting:
     - `NotificationEvent` and `ClaimedJob` are frozen, `not gc.is_tracked(...)`, round-trip through `vertebra.utils.serializers`, and reject attribute mutation with `AttributeError`.
     - Minimal conforming stand-in classes satisfy both runtime `isinstance(obj, <Protocol>)` and static Mypy assignment for `SyncPipelineCapable`, `AsyncPipelineCapable`, `NotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, and `EmbeddingPushdown`, while `SyncSQLiteDriver` (which has no `pipeline()` method) evaluates `isinstance(driver, SyncPipelineCapable) is False` and `isinstance(driver, AsyncPipelineCapable) is False`.
     - All capability protocols and structs resolve lazily from `vertebra.db` and directly from `vertebra.db.driver`.

2. **Implement `src/py/vertebra/db/driver/capabilities.py` and exports (GREEN)**:
   - Implement `NotificationEvent(FrozenBaseStruct, frozen=True, gc=False)` with inline field docstrings:
     - `channel: str`, `payload: bytes`, `sender_id: str | None = None`, `metadata: dict[str, Any] = msgspec.field(default_factory=dict)`.
   - Implement `ClaimedJob(FrozenBaseStruct, frozen=True, gc=False)` with inline field docstrings:
     - `job_id: str`, `queue: str`, `task_name: str`, `payload: bytes`, `attempt: int`, `max_attempts: int`, `lease_until_ms: int`, `headers: dict[str, str] = msgspec.field(default_factory=dict)`.
   - Implement `@runtime_checkable` protocols matching `spec.md` §2.2 verbatim:
     - `SyncPipeline(Protocol)`: `def __enter__(self) -> Self`, `def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None`, `def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None`, `def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None`, `def results(self) -> list[Any]`.
     - `AsyncPipeline(Protocol)`: `async def __aenter__(self) -> Self`, `async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None`, `def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None`, `def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None`, `async def results(self) -> list[Any]`.
     - `SyncPipelineCapable(Protocol)`: `def pipeline(self) -> SyncPipeline: ...`.
     - `AsyncPipelineCapable(Protocol)`: `def pipeline(self) -> AsyncPipeline: ...`.
     - `NotificationSource(Protocol)`: `async def publish(self, channel: str, payload: bytes | str) -> None`, `async def subscribe(self, channels: Sequence[str]) -> None`, `async def unsubscribe(self, channels: Sequence[str]) -> None`, `def listen(self) -> AsyncIterator[NotificationEvent]`.
     - `SyncChangeSource(Protocol)`: `def stream(self) -> Iterator[ChangeEvent]`.
     - `AsyncChangeSource(Protocol)`: `def stream(self) -> AsyncIterator[ChangeEvent]`.
     - `QueueStore(Protocol)`: `async def ensure_schema(self) -> None`, `async def enqueue(self, queue: str, task_name: str, payload: bytes, *, job_id: str | None = None, scheduled_at_ms: int | None = None, max_attempts: int = 3, headers: Mapping[str, str] | None = None) -> str`, `async def enqueue_many(self, queue: str, items: Sequence[tuple[str, bytes]], *, scheduled_at_ms: int | None = None, max_attempts: int = 3) -> list[str]`, `async def claim(self, queue: str, *, limit: int = 1, lease_ms: int = 30_000, worker_id: str = "") -> list[ClaimedJob]`, `async def heartbeat(self, job_id: str, attempt: int, *, extend_ms: int = 30_000) -> bool`, `async def complete(self, job_id: str, attempt: int) -> bool`, `async def fail(self, job_id: str, attempt: int, *, error: str, retry_delay_ms: int | None = None) -> None`, `async def promote_scheduled(self, queue: str | None = None, *, now_ms: int | None = None, limit: int = 100) -> int`.
     - `EmbeddingPushdown(Protocol)` (typed with `TYPE_CHECKING` imports of `VectorBatchView` and `VectorView` from `vertebra._vertebra` so `vertebra.db` never imports `vertebra.embeddings`): `async def embed_documents(self, texts: Sequence[str], *, task: str = "document", dimensions: int | None = None, normalize: bool = True) -> VectorBatchView`, `async def embed_query(self, text: str, *, dimensions: int | None = None, normalize: bool = True) -> VectorView`, `async def similarity_search(self, query_vector: VectorView | Sequence[float], *, table: str, embedding_column: str = "embedding", top_k: int = 10, metric: str = "cosine") -> list[dict[str, Any]]`.
   - Re-export all capability symbols in `src/py/vertebra/db/driver/__init__.py` and `src/py/vertebra/db/__init__.py`.

3. **Refactor & verify**:
   - Run `uv run python -m tools.docs.scan_public_docstrings`, `make lint-all`, and `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py`.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py -k capability` -> fails (`ModuleNotFoundError: No module named 'vertebra.db.driver.capabilities'`).
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py` -> exits `0`.
  - `make lint-all && make docs` -> exits `0`.

## Acceptance Criteria

- [ ] R7: `SyncPipelineCapable`, `AsyncPipelineCapable`, `SyncPipeline`, `AsyncPipeline`, `NotificationSource`, `NotificationEvent`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, `ClaimedJob`, and `EmbeddingPushdown` are implemented with complete typed signatures in `src/py/vertebra/db/driver/capabilities.py`, exported from `vertebra.db.driver` and `vertebra.db`, and have zero upward imports into `vertebra.embeddings` or `vertebra.framework`.
- [ ] `NotificationEvent` and `ClaimedJob` omit cyclic GC tracking (`gc.is_tracked(...) is False`) and pass docstring scanning.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
