---
type: Spec
flow_id: native-oracle-driver
parent_flow: native-oracle-driver
chapter_id: ch06-notifications-and-embeddings
title: "Chapter 06: TxEventQ / AQ Notification Stream and Vector Embedding Pushdown"
description: Implement the dedicated Rust and Python Oracle 26ai TxEventQ / DBMS_AQ notification pump with out-of-band break_execution wakeup and exponential reconnect backoff, plus native VectorView extraction in OracleEmbeddingPushdown.
status: stable
state: in_progress
planning_status: complete
chapter_order: 6
depends_on_chapters:
  - ch05-pipelines-tenancy-and-apex
replan_trigger: null
current_task: "native-oracle-driver:ch06:t02"
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-09T00:48:00Z"
updated_at: "2026-11-09T01:28:30Z"
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
    title: Public contracts for OracleNotificationSource and OracleEmbeddingPushdown
  - id: crate-research
    resource: .agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md
    title: Primary-source evaluation of official pure-Rust oracledb =26.0.0-beta.5 crate
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared AsyncCompletionQueue and independent event-loop drainer
  - id: ora-rs-vector
    resource: src/rs/vertebra-py/src/vector.rs::VectorView
    title: Zero-copy VectorView and VectorBatchView PyO3 classes in vertebra._vertebra
  - id: ora-core-py
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: OracleCore and decode_vector_f32 binary VECTOR codecs
  - id: notify-cap-py
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: NotificationSource and NotificationEvent runtime_checkable capability protocols
  - id: db-cdc-py
    resource: src/py/vertebra/db/cdc.py::ChangeFeed
    title: Engine-neutral ChangeFeed and ChangeEvent envelopes
  - id: pg-notify-py
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: Reference native notification source with identifier validation and ChangeFeed integration
  - id: ora-driver-py
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: SyncOracleDriver and AsyncOracleDriver notification and vector execution hooks
  - id: ora-embeddings-py
    resource: src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown
    title: Oracle 26ai UTL_TO_EMBEDDINGS and VECTOR_DISTANCE pushdown helper
tags:
  - oracle
  - notifications
  - txeventq
  - aq
  - vector
  - embeddings
---

# Chapter 06: TxEventQ / AQ Notification Stream and Vector Embedding Pushdown

<!-- truth: start -->
- **Dedicated Rust AQ / TxEventQ Dequeue Pump (`vertebra-oracle::notify`)**: `AsyncOracleListener` in `src/rs/vertebra-oracle/src/notify.rs` owns a dedicated non-pooled `oracledb::Connection` on a Rust worker thread that runs bounded `DBMS_AQ.DEQUEUE` / TxEventQ raw-payload dequeue iterations, buffers up to `1024` `OracleNotification` frames (`msg_id: bytes`, `payload: bytes`), and wakes blocked waits immediately via out-of-band `connection.break_execution()` on `unsubscribe()` or `close()` [^ora-contracts] [^py-async-rs].
- **Runtime-Checkable `OracleNotificationSource` with Exponential Reconnect Backoff**: `OracleNotificationSource` in `src/py/vertebra/db/adapters/oracle/notify.py` implements the `@runtime_checkable NotificationSource` protocol (`publish`, `subscribe`, `unsubscribe`, `listen() -> AsyncIterator[NotificationEvent]`) with optional `change_feed: ChangeFeed | None = None` from `vertebra.db.cdc`, `decode_json`/`encode_json` from `vertebra.utils.serializers`, AQ convenience helpers (`subscribe_aq`, `publish_aq`, `listen_aq`, `close`), and bounded exponential reconnect backoff (`reconnect_initial_delay` -> `reconnect_max_delay`) [^notify-cap-py] [^db-cdc-py] [^pg-notify-py].
- **Signature-Preserving `VectorView` Embedding Pushdown**: `OracleEmbeddingPushdown` in `src/py/vertebra/db/adapters/oracle/embeddings.py` preserves its public `embed_documents -> VectorBatchView`, `embed_query -> VectorView`, and `similarity_search -> list[dict[str, Any]]` signatures while extending `_extract_vector` and `_extract_batch_vectors` to unwrap native `VectorView` instances, `ORACLE_VECTOR_MAGIC` `bytes`/`memoryview` (`OracleCore.decode_vector_f32`), and dict/tuple row shapes returned by `_oracle` [^ora-rs-vector] [^ora-core-py] [^ora-embeddings-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Chapter Scope & Upstream Dependencies

This chapter implements Requirement `R12` from the [root specification](../../spec.md) and Section 5 of [contracts.md](../../contracts.md). It depends on [Chapter 05 (`ch05-pipelines-tenancy-and-apex`)](../ch05-pipelines-tenancy-and-apex/spec.md), which finalized `SyncOracleDriver` / `AsyncOracleDriver` transaction and tenancy semantics.

Specifically, this chapter delivers:
1. The dedicated Rust `DBMS_AQ.DEQUEUE` / TxEventQ listener pump (`src/rs/vertebra-oracle/src/notify.rs`), module registration in `src/rs/vertebra-oracle/src/lib.rs`, and typing in `src/py/vertebra/_oracle.pyi` (`ch06:t01`).
2. The Python `OracleNotifyConfig` and `@runtime_checkable NotificationSource`-compliant `OracleNotificationSource` implementation in `src/py/vertebra/db/adapters/oracle/notify.py` alongside `SyncOracleDriver` / `AsyncOracleDriver` notification hooks in `src/py/vertebra/db/adapters/oracle/driver.py` (`ch06:t02`).
3. Native `VectorView` and binary `VECTOR` extraction in `OracleEmbeddingPushdown._extract_vector` and `_extract_batch_vectors` in `src/py/vertebra/db/adapters/oracle/embeddings.py` (`ch06:t03`).

### 1.2 Verified Code Seams

| Area | Existing Symbol (`path::Symbol`) | Verified Consequence |
| :--- | :--- | :--- |
| **Async Completion Bridge** | [py_async.rs](../../../../../../src/rs/vertebra-db/src/py_async.rs)`::spawn_rust_db_task_with_attr` [^py-async-rs] | Delivers `AsyncOracleListener.next_notification` completions through `_vertebra_oracle_completion_drainer` without holding the GIL during `DBMS_AQ.DEQUEUE` waits. |
| **Notification Capability** | [capabilities.py](../../../../../../src/py/vertebra/db/driver/capabilities.py)`::{NotificationSource,NotificationEvent}` [^notify-cap-py], [cdc.py](../../../../../../src/py/vertebra/db/cdc.py)`::ChangeFeed` [^db-cdc-py] | `NotificationSource` is `@runtime_checkable` and requires `publish(self, channel: str, payload: bytes | str) -> None`, `subscribe(self, channels: Sequence[str]) -> None`, `unsubscribe(self, channels: Sequence[str]) -> None`, and `listen(self) -> AsyncIterator[NotificationEvent]` (`payload: bytes`); `ChangeFeed` lives in `vertebra.db.cdc`. |
| **Postgres Notify Reference** | [notify.py](../../../../../../src/py/vertebra/db/adapters/postgres/notify.py)`::PostgresNotificationSource` [^pg-notify-py] | Establishes Vertebra's `NotificationSource` implementation, `change_feed: ChangeFeed | None = None` constructor keyword, and channel validation pattern. |
| **Oracle Embeddings & VectorView** | [embeddings.py](../../../../../../src/py/vertebra/db/adapters/oracle/embeddings.py)`::OracleEmbeddingPushdown` [^ora-embeddings-py], [vector.rs](../../../../../../src/rs/vertebra-py/src/vector.rs)`::VectorView` [^ora-rs-vector] | `_extract_vector(row, dimensions)` currently checks only `isinstance(cell, (bytes, bytearray, memoryview))` or `isinstance(cell, str)` before checking `isinstance(cell, (tuple, list))`; when `SyncOracleDriver` / `AsyncOracleDriver` return a `VectorView`, `_extract_vector` must unwrap `cell.to_list()` before the sequence check. |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Rust AQ / TxEventQ Dequeue Pump (`vertebra._oracle.AsyncOracleListener`)

`src/rs/vertebra-oracle/src/notify.rs` defines `OracleNotification` (`#[pyclass(frozen, module = "vertebra._oracle")]` with `queue_name: str`, `consumer_name: str | None`, `msg_id: bytes`, `payload: bytes`, `enqueued_at_epoch_ms: int | None`) and `AsyncOracleListener` (`#[pyclass(frozen, module = "vertebra._oracle")]`):

- `AsyncOracleClient.open_listener(*, queue_name: str, consumer_name: str | None = None, wait_timeout_seconds: int = 5, max_buffered: int = 1024) -> Awaitable[AsyncOracleListener]` opens a dedicated non-pooled `oracledb::Connection` on its own worker thread so blocking `DBMS_AQ.DEQUEUE` waits never starve pooled query workers.
- The worker executes bounded PL/SQL dequeue iterations (`DBMS_AQ.DEQUEUE` with `dequeue_options.wait := :wait_seconds`, `dequeue_options.navigation := DBMS_AQ.NEXT_MESSAGE`, `dequeue_options.visibility := DBMS_AQ.IMMEDIATE`) and returns raw `msg_id: bytes` and `payload: bytes` inside `OracleNotification`.
- `ORA-25228` (timeout or end-of-fetch during `DBMS_AQ.DEQUEUE`) is treated as a normal idle poll boundary on the worker thread and loops immediately while the listener remains active; if the inbound buffer reaches `max_buffered` (`1024`), the oldest buffered frame is dropped (`oldest-drop` backpressure) and `dropped_count` increments.
- `await listener.next_notification(timeout=...)` pops the next buffered `OracleNotification` or registers a oneshot waiter bridged via `spawn_rust_db_task_with_attr` (`_vertebra_oracle_completion_drainer`).
- `await listener.close()` sets the shutdown flag and invokes out-of-band `connection.break_execution()` so a blocked `DBMS_AQ.DEQUEUE` call wakes immediately (`ORA-01013` suppressed on clean close) and joins the worker thread cleanly.

### 2.2 Python `OracleNotificationSource` & Reconnect Backoff (`oracle/notify.py`)

`src/py/vertebra/db/adapters/oracle/notify.py` exports `OracleNotifyConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True)` and `OracleNotificationSource(NotificationSource)`:

```python
class OracleNotifyConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    queue_prefix: str = "VTB_AQ_"
    consumer_name: str | None = None
    wait_timeout_seconds: int = 5
    reconnect_initial_delay: float = 0.25
    reconnect_max_delay: float = 15.0
    max_reconnect_attempts: int = 5
    poll_timeout: float = 1.0
```

- **Protocol Compliance (`isinstance(source, NotificationSource) is True`)**:
  - `__init__(self, driver: AsyncOracleDriver | SyncOracleDriver, config: OracleNotifyConfig | None = None, *, change_feed: ChangeFeed | None = None) -> None`
  - `async def publish(self, channel: str, payload: bytes | str) -> None`
  - `async def subscribe(self, channels: Sequence[str]) -> None`
  - `async def unsubscribe(self, channels: Sequence[str]) -> None`
  - `def listen(self) -> AsyncIterator[NotificationEvent]` (yielding `NotificationEvent(channel=..., payload=..., metadata=...)` where `payload: bytes`)
- **AQ / TxEventQ Convenience Helpers**:
  - `async def subscribe_aq(self, queue_name: str, *, consumer_name: str | None = None) -> None`
  - `async def publish_aq(self, queue_name: str, payload: bytes | str, *, correlation_id: str | None = None) -> str`
  - `def listen_aq(self, queue_name: str | None = None, *, consumer_name: str | None = None) -> AsyncIterator[NotificationEvent]`
  - `async def close(self) -> None`
- **Identifier Validation & Serializers**: Queue and channel names are validated against `^[A-Za-z_][A-Za-z0-9_$#]{0,127}$` before constructing PL/SQL `DBMS_AQ.ENQUEUE` or `DBMS_AQ.DEQUEUE` calls; invalid names raise `ProgrammingError` (`sqlstate="42602"`). JSON serialization uses `decode_json` and `encode_json` from `vertebra.utils.serializers`.
- **Exponential Reconnect Backoff**: During `listen()` / `listen_aq()`, transient `OperationalError` failures close the broken listener, sleep `min(reconnect_max_delay, reconnect_initial_delay * (2 ** (attempt - 1)))` up to `max_reconnect_attempts`, and re-open the listener; `GeneratorExit`, `asyncio.CancelledError`, or `await source.close()` closes all open `AsyncOracleListener` handles in `finally:`.

### 2.3 Signature-Preserving `VectorView` Extraction in `OracleEmbeddingPushdown` (`oracle/embeddings.py`)

`OracleEmbeddingPushdown` in `src/py/vertebra/db/adapters/oracle/embeddings.py` preserves its public method signatures (`embed_documents(...) -> VectorBatchView`, `embed_batch(...) -> VectorBatchView`, `embed_query(...) -> VectorView`, `similarity_search(...) -> list[dict[str, Any]]`) while extending `_extract_vector(row: Any, dimensions: int) -> list[float]` and `_extract_batch_vectors(cls, rows: Sequence[Any], expected_count: int, dimensions: int) -> list[list[float]]` to handle all row and vector shapes returned by native `vertebra._oracle`:
1. Native `VectorView` (`isinstance(cell, VectorView)` from `vertebra._vertebra`): unwraps `cell = cell.to_list()` before the sequence check.
2. Raw binary `bytes | bytearray | memoryview` starting with `ORACLE_VECTOR_MAGIC` (`0xDB`): decodes via `decode_oracle_vector_f32(bytes(cell))` / `OracleCore.decode_vector_f32(bytes(cell))`.
3. `Sequence[float | int]` or JSON-encoded string (`decode_json(cell)`), raising `DataError("Oracle embedding row has no valid vector")` when `cell` is missing or shorter than `dimensions`.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/6 files documented]` (user documentation in `docs/usage/databases.md`, `README.md`, and `AGENTS.md` and OKF knowledge chapters in `database-drivers-and-dialers.md`, `query-pipeline-cdc-and-tenancy.md`, and `tech-stack.md` are updated in [Chapter 07 (`ch07-conformance-and-okf-synthesis`)](../ch07-conformance-and-okf-synthesis/spec.md) tasks `ch07:t03`–`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task ID | Worksheet | Verification Strategy | Public Seam / Gate |
| :--- | :--- | :--- | :--- | :--- |
| R12 (Rust AQ / TxEventQ Dequeue Pump) | `native-oracle-driver:ch06:t01` | [t01-rust-and-python-aq-notification-pump.md](tasks/t01-rust-and-python-aq-notification-pump.md) | `behavior_tdd` | `src/rs/vertebra-oracle/src/notify.rs`, `src/py/tests/unit/db/test_oracle_adapter.py` |
| R12 (`OracleNotificationSource` & Reconnect Backoff) | `native-oracle-driver:ch06:t02` | [t02-oracle-notification-source-and-backoff.md](tasks/t02-oracle-notification-source-and-backoff.md) | `behavior_tdd` | `src/py/vertebra/db/adapters/oracle/notify.py`, `src/py/tests/unit/db/test_oracle_adapter.py` |
| R12 (`OracleEmbeddingPushdown` `VectorView` Integration) | `native-oracle-driver:ch06:t03` | [t03-oracle-embedding-pushdown-vector-view.md](tasks/t03-oracle-embedding-pushdown-vector-view.md) | `behavior_tdd` | `src/py/vertebra/db/adapters/oracle/embeddings.py`, `src/py/tests/unit/db/test_oracle_adapter.py` |

## 5. Implementation Plan

- [x] native-oracle-driver:ch06:t01 — Implement Rust and Python AQ / TxEventQ Notification Pump (tasks/t01-rust-and-python-aq-notification-pump.md)
- [~] native-oracle-driver:ch06:t02 — Implement OracleNotificationSource and Reconnect Backoff (tasks/t02-oracle-notification-source-and-backoff.md)
- [ ] native-oracle-driver:ch06:t03 — Align OracleEmbeddingPushdown with Native VectorView (tasks/t03-oracle-embedding-pushdown-vector-view.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle && uv run pytest src/py/tests/unit/db/test_oracle_adapter.py && make lint-all` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: `in_progress` (`1/3` tasks completed); `current_task: "native-oracle-driver:ch06:t02"`.
- **Upstream Prerequisite**: [Chapter 05 (`ch05-pipelines-tenancy-and-apex`)](../ch05-pipelines-tenancy-and-apex/spec.md) (`native-oracle-driver:ch05:t03`).
- **Downstream Consumer**: [Chapter 07 (`ch07-conformance-and-okf-synthesis`)](../ch07-conformance-and-okf-synthesis/spec.md) (`native-oracle-driver:ch07:t01`).

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public contracts for `OracleNotificationSource` and `OracleEmbeddingPushdown`.
[^crate-research]: `.agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md` — Primary-source evaluation of official pure-Rust `oracledb =26.0.0-beta.5` crate.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared `AsyncCompletionQueue` and independent event-loop drainer.
[^ora-rs-vector]: `src/rs/vertebra-py/src/vector.rs::VectorView` — Zero-copy `VectorView` and `VectorBatchView` PyO3 classes in `vertebra._vertebra`.
[^ora-core-py]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — `OracleCore` and `decode_vector_f32` binary `VECTOR` codecs.
[^notify-cap-py]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — `NotificationSource` and `NotificationEvent` `runtime_checkable` capability protocols.
[^db-cdc-py]: `src/py/vertebra/db/cdc.py::ChangeFeed` — Engine-neutral `ChangeFeed` and `ChangeEvent` envelopes.
[^pg-notify-py]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — Reference native notification source with identifier validation and `ChangeFeed` integration.
[^ora-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — `SyncOracleDriver` and `AsyncOracleDriver` notification and vector execution hooks.
[^ora-embeddings-py]: `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown` — Oracle 26ai `UTL_TO_EMBEDDINGS` and `VECTOR_DISTANCE` pushdown helper.
