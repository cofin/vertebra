---
type: Task
id: "native-postgres-driver:ch36:t04"
chapter_id: ch36-replication-python-bridge
title: "Deliver cancellation-safe event leases"
description: "Expose next through supervisor delivery leases, convert before publish, and complete cancellation/publication and generation-fenced acknowledgement behavior through the actual native extension"
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: ["native-postgres-driver:ch36:t03"]
read_set:
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/tests/unit/db/test_postgres_native_replication.py
  - src/py/tests/unit/db/test_postgres_native_notifications.py
files:
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/py/vertebra/_postgres.pyi
  - src/rs/vertebra-db/src/py_async.rs
tests:
  - src/py/tests/unit/db/test_postgres_native_replication.py
estimated_loc: 245
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
commit: null
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

# Task t04: Deliver cancellation-safe event leases

## Objective & Scope Boundary

Expose next through supervisor delivery leases, convert before publish, and complete cancellation/publication and generation-fenced acknowledgement behavior through the actual native extension. [^contracts] [^async] [^identity]

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/replication/mod.rs` — Read the owning public seam and existing call-site conventions.
- `src/rs/vertebra-postgres/src/replication/supervisor.rs` — Read the owning public seam and existing call-site conventions.
- `src/rs/vertebra-db/src/py_async.rs` — Read the owning public seam and existing call-site conventions.
- `src/py/vertebra/_postgres.pyi` — Read the owning public seam and existing call-site conventions.
- `src/py/tests/unit/db/test_postgres_native_replication.py` — Explicit new canonical target created by t01; later tasks extend it.
- `src/py/tests/unit/db/test_postgres_native_notifications.py` — Read the owning public seam and existing call-site conventions.

Owning chapter spec and referenced local pyo3-bridge skill/FFI knowledge supply shared contracts; no undeclared source exploration is required. New methods/converter/class are declared creation targets, not assumed baseline symbols.

## Symbol & Signature Contracts

`AsyncPostgresReplication.next(self)->Awaitable[PostgresReplicationRecord|None]` uses the new future-aware aborting helper and schedules `supervisor.next_event()` and transfers its delivery lease into map_ok. Exact order: synchronously claim the handle; await and retain delivery; build Python object from event(); recheck original Future.cancelled(); call publish(); on control success record its generation; return already-built object. All failures/drop restore through upstream lease; no bridge-owned duplicate front buffer. Concurrent next rejects until outcome publication/drop, including queued completed workers. acknowledge snapshots last published control generation at invocation. No Python attachment in workers.

`pub fn spawn_rust_db_task_with_attr_and_future<F,T,E,M,EMap>(py: Python<'_>, fut: F, map_ok: M, map_err: EMap, attr_name: &'static str) -> PyResult<RustDbTaskAwaitable>` in `vertebra-db/src/py_async.rs`, with `F: Future<Output=Result<T,E>>+Send+'static`, `T:Send+'static`, `E:Send+'static`, `M: for<'py> FnOnce(Python<'py>, Bound<'py,PyAny>, T)->PyResult<Py<PyAny>>+Send+'static`, `EMap: FnOnce(Python<'_>,E)->PyErr+Send+'static`. The second mapper argument is the exact original `asyncio.Future` owned by the returned awaitable. This variant retains the existing abort-on-cancel/unawaited-drop behavior. Refactor `spawn_rust_db_task_with_attr` to delegate to it with an adapter ignoring the future, preserving existing callers and exception behavior; do not duplicate the full task/drainer implementation or change the non-aborting `and_cancel` variant. Capture a separate `Py<PyAny>` clone of the same future for the outcome closure, bind it only inside the event-loop producer, and keep `CompletedDbTask.future` unchanged.

Add `ReplicationNextClaim` (private RAII owner of `Arc<AtomicBool>`): acquire synchronously at `next()` invocation using compare_exchange(false,true,AcqRel,Acquire), returning InterfaceError if claimed. Move it through worker waiting and queued mapper; Drop releases with store(false,Release), including setup failure, worker abort, conversion failure and dropped outcomes. A second next in the same event-loop turn must fail even before Tokio polls either future. The supervisor lease's own reservation remains independent defense, not the Python invocation-order mechanism.

`next()` uses the future-aware helper and retains both claim and delivery lease through every Python constructor. Inside mapper: create Python record from `delivery.event()`; recheck `original_future.cancelled()` immediately after all fallible/reentrant Python work; if true drop delivery and return CancelledError without publishing; otherwise `delivery.publish()` validates generation and registers the commit, update the last successfully published control generation, then return the prebuilt object. No fallible Python calls follow publish. The supported cancellation boundary is the owning Python event loop; direct `Future.cancel()` from another thread is unsupported (use call_soon_threadsafe). A genuine constructor-triggered owning-loop cancellation is covered without mocking the native handle, worker, lease or mapper.

- All Python constructors and conversions run on the Python event loop inside `DbOutcomeProducer`; Tokio workers do not call `Python::attach`.
- The existing module drainer attribute from `async_client.rs` is reused. Stubs use `def ... -> Awaitable[T]`, matching `RustDbTaskAwaitable`.
- Dedicated sockets reject pooler modes `transaction`, `statement`, and `auto` before dialing. An `AsyncPostgresPool`-acquired client supplies these modes in tests; direct constructors default to `direct`.

## Edge Cases & Error Semantics

Queue capacity is 1..1024 inclusive; bool is TypeError and 0/1025 are InterfaceError before dialing. Invalid LSN bool/integer bounds reject before socket I/O; canonical InterfaceError covers pooler refusal, overlap, stale generations and post-terminal calls. Conversion errors leave the event lease unpublished. Socket cancellation drops owned transports; close never advances acknowledged LSN. Terminal next error preserves its class and cause, with closed snapshot true.

## Public-Seam Test Blueprint

New canonical Python module `src/py/tests/unit/db/test_postgres_native_replication.py` exercises actual installed extension against a local scripted protocol peer. Fixtures follow the existing notification module; the native client/supervisor are never mocked.

For constructor reentrancy, use an isolated Python subprocess so cached PyOnceLock types cannot hide the injection. Before the first native identity/tuple conversion, replace the Python PostgresReplicationRecord constructor collaborator with a wrapper that calls the real constructor, cancels the captured original asyncio.Future, and returns the real constructed record. Capture that real future through a temporary event-loop create_future factory wrapper; arm cancellation only for the target Commit. The native supervisor, delivery lease, helper, and mapper remain real. Assert refusal to acknowledge and replay of the same Commit through next, then restore the factory and constructor. This tests the owning-loop reentrant path without an unsupported foreign-thread Future.cancel.

| Case | Input | Expected output/error |
| --- | --- | --- |
| exact record order | control0, Begin, Update(amount9.50/TOAST), Commit0x300, Beginxid9 | same order and lossless objects; commit acknowledgement enabled only after delivery |
| cancelled queued commit | queue outcome then cancelPythonFuture before drainer runs | acknowledgement0x300 fails; followingnext returns sameCommit thenBegin9; acknowledgement then succeeds |
| same-turn invocation order | a=handle.next(); b=handle.next() before yielding the Python loop | second invocation InterfaceError; first owns the reservation until success/drop |
| reentrant constructor cancellation | Python record constructor cancels the original future after worker completion, before mapper returns | retained Commit remains unacknowledgeable, followingnext returns the identical Commit then Begin9; zero wire advancement |
| waiter overlap | first next queued or awaiting peer, second next | InterfaceError; cancellation releases reservation without reordering |
| stale generation | old outcome queued during reconnect | no stale object; next returns new control before replacement records; staleack fails |
| terminal vs recoverable drop | reconnectFalse vsTrue | ConnectionFailure+closedTrue vs closedFalse/controlgeneration1 |
| cancel then close | queuedCommit cancelled, immediatelyaclose | closedTrue, no late flush/apply advancement, pendingwait resolvesNone |
| full queue | capacity2, stalledconsumer, periodicwriter status | Python loop responsive; acknowledgedLSN unchanged; keepalive behind unreadWAL waits for capacity |

## Verification

Initial Red: create/extend canonical behavioral cases; run `UV_NO_SYNC=1 PYO3_PYTHON="$PWD/.venv/bin/python" make dev` then `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_replication.py -k native_replication_next -q` and observe absent behavior. Final Green: rebuild again after Rust edits, rerun focused selector and full module, then `uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_replication.py`, `uv run --no-sync mypy src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_replication.py`, `uv run --no-sync python -m mypy.stubtest vertebra._postgres`, and PostgreSQL all-target Clippy. All commands exit0; preserve UV_NO_SYNC=1 and PYO3_PYTHON. No skip/xfail or importorskip masks the new API.

## Acceptance Criteria

- [ ] The exact public seam satisfies every literal case and refusal path above (R22.6/R23.1).
- [ ] Fresh Red and Green plus fast lint/type or native Clippy evidence is recorded with exit codes.
- [ ] Event-loop conversion, Drop ownership, and feedback fencing remain consistent with chapter section2; owned stubs/docstrings match landed methods.

## Notes & Discoveries

The canonical Python raw replication module is absent at the planning base and is explicitly created in t01; PostgresChangeSource remains a separate downstream ch24 module. No production behavior is verified by this planning artifact.

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Raw replication ownership and checkpoint boundary.
[^async]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Aborting worker cancellation and event-loop outcome delivery.
[^identity]: `src/rs/vertebra-postgres/src/replication/mod.rs::verified_identity_to_python` — Existing pure event-loop identity and tuple conversion seam.
