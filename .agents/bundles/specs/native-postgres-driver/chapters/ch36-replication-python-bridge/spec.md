---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch36-replication-python-bridge
title: "Chapter 36: Cancellation-safe native replication Python bridge"
description: "Expose identity probes and lease-backed raw replication handles over the existing PyO3 completion drainer."
status: stable
state: planned
planning_status: complete
depends_on_chapters: [ch35-replication-supervision]
replan_trigger: null
current_task: "native-postgres-driver:ch36:t01"
last_verified_checkpoint: null
last_reviewed_commit: null
plan_revision: 8
plan_commit: d90d4625
state_revision: 0
created_at: "2026-11-12T04:20:00Z"
updated_at: "2026-11-12T04:25:15Z"
generated: {by: flow-revise, at: "2026-11-12T04:20:00Z"}
verified: []
sources:
  - {id: contracts, resource: .agents/bundles/specs/native-postgres-driver/contracts.md, title: Raw replication ownership contracts}
  - {id: async, resource: "src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr", title: Cancellation-safe async task bridge}
  - {id: identity, resource: "src/rs/vertebra-postgres/src/replication/mod.rs::verified_identity_to_python", title: Event-loop Python conversion}
tags: [postgres, replication, pyo3, cancellation]
---

# Cancellation-safe Native Replication Python Bridge

<!-- truth: start -->
- Raw handles own dedicated replication sockets and optional raw reconnect; durable checkpoint CAS and source retries belong to ch24 [^contracts].
- Delivery leases remain unpublished until conversion succeeds on the event loop; cancelling queued completions restores records without advancing feedback [^async].
- Identity and row converters retain lossless adapter-owned values and immutable record envelopes [^identity].
<!-- truth: end -->

## 1. Specification, Context & Provenance

The Rust session and supervisor are available after reviewed ch34/ch35. Python callers need raw identity probes and stream handles without worker-thread Python attachment, socket leaks, or cancellation-induced data loss. The existing notification bridge establishes the completion queue pattern. The current replication module exposes identity/tuple converters and an empty registration hook; the new raw replication test module is an explicitly new canonical target. PostgresChangeSource and durable CAS remain ch24 scope.

Relevant skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md). Relevant knowledge: [FFI discipline](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [CDC](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md).

## 2. Requirements, Concrete Contracts & Architecture

- R22.6: expose dedicated identity and raw replication APIs with pooler refusal and bounded ownership.
- R23.1: preserve event payload types and cancellation-safe publication/ordering across drainer handoff.

### Public Python methods

```python
class AsyncPostgresClient:
    def replication_identity(self, slot: str, publication: str) -> Awaitable[PostgresReplicationIdentity]: ...
    def open_replication(self, slot: str, publication: str, *, start_lsn: int | None = None, queue_capacity: int = 1024, reconnect: bool = True) -> Awaitable[AsyncPostgresReplication]: ...
class AsyncPostgresReplication:
    @property
    def closed(self) -> bool: ...
    def next(self) -> Awaitable[PostgresReplicationRecord | None]: ...
    def acknowledge(self, commit_end_lsn: int) -> Awaitable[None]: ...
    def aclose(self) -> Awaitable[None]: ...
    def __aenter__(self) -> Awaitable[AsyncPostgresReplication]: ...
    def __aexit__(self, exc_type: Any = None, exc_val: Any = None, exc_tb: Any = None) -> Awaitable[None]: ...
```

The Rust methods return `PyResult<Bound<'py, RustDbTaskAwaitable>>` (or the equivalent existing client `RustDbTaskAwaitable` return shape). `start_lsn` and acknowledgements accept `0..2**64-1` integers, rejecting bool with TypeError and out-of-range values with ValueError. `queue_capacity` accepts exact integers in 1..1024 inclusive; bool is TypeError and out-of-range values are InterfaceError before opening replication sockets.

### Rust ownership and publication

`#[pyclass(name="AsyncPostgresReplication", module="vertebra._postgres", frozen, immutable_type)] pub struct PyAsyncPostgresReplication` stores `Arc<ReplicationSupervisor>` and an atomic last successfully published control generation. `replication::register_pymodule` registers it. A generation value is initialized only when the initial control event is successfully delivered; `acknowledge` without a published control is InterfaceError. Normal EOF of a pending `next()` resolves None; a new `next()` after terminal closure is InterfaceError. Transport/protocol errors retain canonical error classes and `closed=True`; reconnect backoff leaves `closed=False`.

The supervisor owns exact APIs from ch35: `async next_event(&self)->Result<Option<ReplicationEventDelivery>,PostgresError>`, `acknowledge(&self,generation:u64,commit_end_lsn:u64)->Result<u64,PostgresError>`, `async shutdown(&self)->Result<(),PostgresError>`, `is_closed(&self)->bool`, `cancel(&self)->()` (synchronous cancellation/fencing, no ambient runtime lookup or network). `ReplicationEventDelivery::event(&self)->&ReplicationStreamEvent`, `publish(self)->Result<ReplicationStreamEvent,PostgresError>`; dropping the lease restores its unconsumed event and releases the sole-next reservation, unless stale or closed. A reservation covers worker waiting AND queued Python completion. A second overlapping next is InterfaceError throughout that interval.

`next()` synchronously claims its handle before spawning and returns the lease from Tokio into the future-aware mapper; inside it, first convert `lease.event()` to an owned Python object, then recheck the original future cancelled state after all constructors, then call `lease.publish()` and update the control generation only on success, then return the already-built object with no further fallible Python work. Conversion failure, cancelled waiter, dropped drainer outcome, or stale generation never publish a commit. The lease replaces a duplicate bridge-specific pending-record buffer. Acknowledge captures the last published control generation at method invocation, never the latest supervisor resume generation, so reconnect cannot retarget a stale acknowledgement.

Opening uses a pending ownership guard around the new supervisor, transferred into `map_ok`. Its Drop calls `ReplicationSupervisor::cancel()` synchronously and releases ownership; it must not rely on entering an ambient Tokio runtime or Python. Async close joins tasks and performs final feedback/CopyDone under the configured timeout; Drop safely cancels ownership when that full close cannot be awaited. Repeated aclose resolves None and context-manager exit follows the same path.

### Future-aware delivery and invocation-order contract

`pub fn spawn_rust_db_task_with_attr_and_future<F,T,E,M,EMap>(py: Python<'_>, fut: F, map_ok: M, map_err: EMap, attr_name: &'static str) -> PyResult<RustDbTaskAwaitable>` in `vertebra-db/src/py_async.rs`, with `F: Future<Output=Result<T,E>>+Send+'static`, `T:Send+'static`, `E:Send+'static`, `M: for<'py> FnOnce(Python<'py>, Bound<'py,PyAny>, T)->PyResult<Py<PyAny>>+Send+'static`, `EMap: FnOnce(Python<'_>,E)->PyErr+Send+'static`. The second mapper argument is the exact original `asyncio.Future` owned by the returned awaitable. This variant retains the existing abort-on-cancel/unawaited-drop behavior. Refactor `spawn_rust_db_task_with_attr` to delegate to it with an adapter ignoring the future, preserving existing callers and exception behavior; do not duplicate the full task/drainer implementation or change the non-aborting `and_cancel` variant. Capture a separate `Py<PyAny>` clone of the same future for the outcome closure, bind it only inside the event-loop producer, and keep `CompletedDbTask.future` unchanged.

Add `ReplicationNextClaim` (private RAII owner of `Arc<AtomicBool>`): acquire synchronously at `next()` invocation using compare_exchange(false,true,AcqRel,Acquire), returning InterfaceError if claimed. Move it through worker waiting and queued mapper; Drop releases with store(false,Release), including setup failure, worker abort, conversion failure and dropped outcomes. A second next in the same event-loop turn must fail even before Tokio polls either future. The supervisor lease's own reservation remains independent defense, not the Python invocation-order mechanism.

`next()` uses the future-aware helper and retains both claim and delivery lease through every Python constructor. Inside mapper: create Python record from `delivery.event()`; recheck `original_future.cancelled()` immediately after all fallible/reentrant Python work; if true drop delivery and return CancelledError without publishing; otherwise `delivery.publish()` validates generation and registers the commit, update the last successfully published control generation, then return the prebuilt object. No fallible Python calls follow publish. The supported cancellation boundary is the owning Python event loop; direct `Future.cancel()` from another thread is unsupported (use call_soon_threadsafe). A genuine constructor-triggered owning-loop cancellation is covered without mocking the native handle, worker, lease or mapper.

### Lossless event payload

`pub fn replication_event_to_python(py:Python<'_>, event:&ReplicationStreamEvent)->PyResult<Py<PyAny>>` extends existing public tuple/identity converters. It returns the existing `PostgresReplicationRecord(kind,payload,commit_end_lsn,generation)` without publishing the lease. Control payload has `identity` (PostgresReplicationIdentity) and `acknowledged_lsn`; all transaction payloads retain `xid` and `dedup_key` tuple `(system_identifier,slot,begin_final_lsn,transaction_ordinal)`. Begin adds `begin_final_lsn`, `commit_time_pg_us`; row events add `schema`, `table`, `relation_id`, `old_kind`, `old` and/or `new` mappings as applicable (old_kind is `key`, `old`, or None). Truncate carries ordered `relations` tuple of `(relation_id,schema,table)`, `cascade`, `restart_identity`. Message carries `transactional`, `message_lsn`, `prefix`, `content:bytes`; nontransactional messages use xid/dedup_key None. Commit retains `flags`, `commit_lsn`, `commit_end_lsn`, `commit_time_pg_us`, `origin_name`, `origin_lsn`. `commit_end_lsn` top-level is non-None only for commit. Tuple conversion reuses `pgoutput_tuple_to_python_dict` and preserves SQL NULL, singleton `PG_UNCHANGED_TOAST`, Decimal, PgInterval/PgArray/PgComposite/PgTemporal/PgJson/PgBitString/PgVector without stringification.

- All Python constructors and conversions run on the Python event loop inside `DbOutcomeProducer`; Tokio workers do not call `Python::attach`.
- The existing module drainer attribute from `async_client.rs` is reused. Stubs use `def ... -> Awaitable[T]`, matching `RustDbTaskAwaitable`.
- Dedicated sockets reject pooler modes `transaction`, `statement`, and `auto` before dialing. An `AsyncPostgresPool`-acquired client supplies these modes in tests; direct constructors default to `direct`.

Caller -> `AsyncPostgresClient` -> standalone identity/supervisor -> session pump. Handle next -> supervisor delivery lease -> event-loop converter -> lease publication -> Python result. Handle acknowledgement -> captured published control generation -> supervisor -> feedback writer.

| Condition | Required result |
| --- | --- |
| queued Commit cancelled | dropped lease restores exact Commit; acknowledgement fails until successful delivery |
| generation changes during conversion | publish fails, object discarded, no stale result/feedback |
| cancelled open before publication | ownership guard cancels all dedicated sockets to peer EOF |
| full queue | Python loop remains responsive; independent heartbeat retains acknowledged positions |
| closing with pending next | pending wait finishes None; no late publication; repeated close succeeds |

Alternatives: reuse the supervisor lease (chosen, one reservation/buffer and small interface); duplicate bridge staging state (extra buffer and cancellation races); immediate publish on worker receipt (violates commit fencing).

## 3. Documentation & OKF Knowledge Plan

`[0/3 files documented]`: `_postgres.pyi` signatures/docstrings land with their methods; chapter sync updates existing `knowledge/patterns/rust/ffi-and-gil-discipline.md` and `knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` in present tense with sources and verified stamps. User CDC/cloud guides remain fully owned by ch30; no runtime completion is claimed during planning.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Canonical gate |
| --- | --- | --- | --- |
| R22.6 identity | t01 | behavior_tdd | native_replication_identity Python tests |
| R23.1 values | t02 | behavior_tdd | replication_bridge_conversion Rust wire tests |
| R22.6 ownership | t03 | behavior_tdd | native_replication_lifecycle Python tests |
| R23.1 delivery | t04 | behavior_tdd | native_replication_next Python tests |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch36:t01 — Expose native replication identity probes (tasks/t01-identity-probe.md)
- [ ] native-postgres-driver:ch36:t02 — Convert lossless replication events on the event loop (tasks/t02-lossless-event-conversion.md)
- [ ] native-postgres-driver:ch36:t03 — Expose owned raw replication handles and lifecycle (tasks/t03-owned-stream-handles.md)
- [ ] native-postgres-driver:ch36:t04 — Deliver cancellation-safe event leases (tasks/t04-cancelled-delivery-leases.md)

**Chapter Recovery Checkpoint:** `UV_NO_SYNC=1 PYO3_PYTHON="$PWD/.venv/bin/python" make dev && uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_replication.py && uv run --no-sync mypy src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_replication.py && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_replication.py -q && PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` exits 0. Preserve UV_NO_SYNC=1 and the same PYO3_PYTHON for every Cargo command.

## 6. Current State & Remaining Plan Snapshot

Plan revision 8 is approved at `d90d4625`; all four pending tasks require completed and reviewed ch35. Native Python replication methods and canonical Python test module are new targets. ch24 consumes the published raw API after chapter code/doc review; source retry, checkpoints, permanent driver cutover and release gates remain downstream.

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Raw replication ownership and checkpoint boundary.
[^async]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Aborting worker cancellation and event-loop outcome delivery.
[^identity]: `src/rs/vertebra-postgres/src/replication/mod.rs::verified_identity_to_python` — Existing pure event-loop identity and tuple conversion seam.
