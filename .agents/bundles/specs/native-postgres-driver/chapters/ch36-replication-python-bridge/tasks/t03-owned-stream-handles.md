---
type: Task
id: "native-postgres-driver:ch36:t03"
chapter_id: ch36-replication-python-bridge
title: "Expose owned raw replication handles and lifecycle"
description: "Expose the raw replication handle with open, ownership, zero-I/O closed, acknowledgement, close and context-manager methods"
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: ["native-postgres-driver:ch36:t02"]
read_set:
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/tests/unit/db/test_postgres_native_replication.py
files:
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_replication.py
estimated_loc: 235
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

# Task t03: Expose owned raw replication handles and lifecycle

## Objective & Scope Boundary

Expose the raw replication handle with open, ownership, zero-I/O closed, acknowledgement, close and context-manager methods. next is added by t04; no record is published in this task. [^contracts] [^async] [^identity]

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/replication/mod.rs` — Read the owning public seam and existing call-site conventions.
- `src/rs/vertebra-postgres/src/async_client.rs` — Read the owning public seam and existing call-site conventions.
- `src/rs/vertebra-postgres/src/replication/supervisor.rs` — Read the owning public seam and existing call-site conventions.
- `src/rs/vertebra-db/src/py_async.rs` — Read the owning public seam and existing call-site conventions.
- `src/py/vertebra/_postgres.pyi` — Read the owning public seam and existing call-site conventions.
- `src/py/tests/unit/db/test_postgres_native_replication.py` — Explicit new canonical target created by t01; later tasks extend it.

Owning chapter spec and referenced local pyo3-bridge skill/FFI knowledge supply shared contracts; no undeclared source exploration is required. New methods/converter/class are declared creation targets, not assumed baseline symbols.

## Symbol & Signature Contracts

`AsyncPostgresClient.open_replication(self,slot:str,publication:str,*,start_lsn:int|None=None,queue_capacity:int=1024,reconnect:bool=True)->Awaitable[AsyncPostgresReplication]`; handle Rust class and methods closed/acknowledge/aclose/__aenter__/__aexit__ match section2. Store supervisor Arc and last-published-control generation state. `acknowledge` before that state is initialized is InterfaceError. Register through existing register_pymodule. Guard pending open before event-loop allocation; disarm only after the Python object owns the supervisor. Drop calls `ReplicationSupervisor::cancel()` synchronously; no ambient runtime lookup. The no-record handle remains safely closable before t04.

- All Python constructors and conversions run on the Python event loop inside `DbOutcomeProducer`; Tokio workers do not call `Python::attach`.
- The existing module drainer attribute from `async_client.rs` is reused. Stubs use `def ... -> Awaitable[T]`, matching `RustDbTaskAwaitable`.
- Dedicated sockets reject pooler modes `transaction`, `statement`, and `auto` before dialing. An `AsyncPostgresPool`-acquired client supplies these modes in tests; direct constructors default to `direct`.

## Edge Cases & Error Semantics

Queue capacity is 1..1024 inclusive; bool is TypeError and 0/1025 are InterfaceError before dialing. Invalid LSN bool/integer bounds reject before socket I/O; canonical InterfaceError covers pooler refusal, overlap, stale generations and post-terminal calls. Conversion errors leave the event lease unpublished. Socket cancellation drops owned transports; close never advances acknowledged LSN. Terminal next error preserves its class and cause, with closed snapshot true.

## Public-Seam Test Blueprint

New canonical Python module `src/py/tests/unit/db/test_postgres_native_replication.py` exercises actual installed extension against a local scripted protocol peer. Fixtures follow the existing notification module; the native client/supervisor are never mocked.

| Case | Input | Expected output/error |
| --- | --- | --- |
| open/close | start_lsn=0x100, capacity2, reconnectFalse | dedicated CopyBoth session, closedFalse; repeated acloseNone, closedTrue, EOF |
| open cancelled | cancel worker or queued outcome before Python object publication | sockets reach EOF; no surviving reader/writer/retry |
| pooler refusal | transaction/statement/auto client | InterfaceError, zero standalone accepts |
| invalid integers | True, -1, 2**64 start_lsn; capacityFalse,0,1025 | bool/LSN TypeError or ValueError; capacity0/1025 InterfaceError before dialing |
| no published control | acknowledge(0x200) immediately afteropen | InterfaceError, flush/apply remain0x100 |
| context exit | asyncwith handle then exit | idempotent same close path, no loop blocking |

## Verification

Initial Red: create/extend canonical behavioral cases; run `UV_NO_SYNC=1 PYO3_PYTHON="$PWD/.venv/bin/python" make dev` then `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_replication.py -k native_replication_lifecycle -q` and observe absent behavior. Final Green: rebuild again after Rust edits, rerun focused selector and full module, then `uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_replication.py`, `uv run --no-sync mypy src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_replication.py`, `uv run --no-sync python -m mypy.stubtest vertebra._postgres`, and PostgreSQL all-target Clippy. All commands exit0; preserve UV_NO_SYNC=1 and PYO3_PYTHON. No skip/xfail or importorskip masks the new API.

## Acceptance Criteria

- [ ] The exact public seam satisfies every literal case and refusal path above (R22.6/R23.1).
- [ ] Fresh Red and Green plus fast lint/type or native Clippy evidence is recorded with exit codes.
- [ ] Event-loop conversion, Drop ownership, and feedback fencing remain consistent with chapter section2; owned stubs/docstrings match landed methods.

## Notes & Discoveries

The canonical Python raw replication module is absent at the planning base and is explicitly created in t01; PostgresChangeSource remains a separate downstream ch24 module. No production behavior is verified by this planning artifact.

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Raw replication ownership and checkpoint boundary.
[^async]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Aborting worker cancellation and event-loop outcome delivery.
[^identity]: `src/rs/vertebra-postgres/src/replication/mod.rs::verified_identity_to_python` — Existing pure event-loop identity and tuple conversion seam.
